# OCPP EV Charging Platform — AWS Architecture Overview

> **Scope:** Production-grade architecture for 50k–150k concurrent EV charging stations  
> **Protocol:** OCPP 1.6J / 2.0.1 over WebSocket (WSS)  
> **Region:** AWS eu-west-1, Multi-AZ

---

## Table of Contents

1. [High-Level Architecture](#1-high-level-architecture)
2. [Network Load Balancer (NLB)](#2-network-load-balancer-nlb)
3. [WebSocket Gateway (WSGW)](#3-websocket-gateway-wsgw)
4. [Redis Cluster — Connection Routing](#4-redis-cluster--connection-routing)
5. [Kafka / MSK — Message Pipeline](#5-kafka--msk-message-pipeline)
6. [Latest-State Store](#6-latest-state-store)
7. [Idempotency & Out-of-Order Event Handling](#7-idempotency--out-of-order-event-handling)
8. [Capacity Planning](#8-capacity-planning)
9. [Disaster Recovery & Failover Scenarios](#9-disaster-recovery--failover-scenarios)
10. [Charger Authentication & mTLS](#10-charger-authentication--mtls)
11. [Key Design Decisions](#11-key-design-decisions)
12. [Telemetry & Data Strategy](#12-telemetry--data-strategy)
13. [Cost Estimate](#13-cost-estimate)
14. [Open Issues & Out-of-Scope Topics](#14-open-issues--out-of-scope-topics)

---

## 1. High-Level Architecture

```
EV Chargers (50k–150k)
        │
        │ WSS/TLS 1.3, port 443 (outbound from charger)
        ▼
  ┌─────────────────────────────────────────────┐
  │         Route53 (latency-based routing)     │
  └─────────────────────┬───────────────────────┘
                        │
  ┌─────────────────────▼───────────────────────┐
  │   Network Load Balancer (NLB, multi-AZ)     │
  │   TCP passthrough — static IP per AZ        │
  └────────┬──────────────┬──────────────┬──────┘
           │              │              │
     AZ: 1a         AZ: 1b         AZ: 1c
  ┌────────▼──┐    ┌───────▼──┐    ┌────▼──────┐
  │ WSGW Pods │    │ WSGW Pods│    │ WSGW Pods │
  │ (2–4 pods)│    │ (2–4 pods│    │ (2–4 pods)│
  └──┬─────┬──┘    └───┬───┬──┘    └──┬─────┬──┘
     │     │           │   │          │     │
     │     └───────────┼───┼──────────┘     │
     │  (A) upstream   │   │  (B) state+routing
     ▼                 ▼   ▼                ▼
┌────────────────┐   ┌─────────────────────────────────────┐
│  Amazon MSK    │   │  State & Routing Layer               │
│  (Kafka, 3 AZ) │   │                                     │
│                │   │  ┌─────────────────────────────┐   │
│  WSGW produces │   │  │ DynamoDB (latest-state)      │   │
│  all OCPP      │   │  │ source of truth, persistent  │   │
│  events        │   │  │ PK: chargePointId            │   │
│                │   │  └──────────────┬──────────────┘   │
└───────┬────────┘   │                 │ read-through       │
        │            │  ┌──────────────▼──────────────┐   │
        │            │  │ Redis Cluster (ElastiCache)  │   │
        │            │  │                             │   │
        │            │  │ cp:state:{id}  ← latest     │   │
        │            │  │ cp:{id}:pod    ← routing    │   │
        │            │  │ cp:{id}:session:* ← live    │   │
        │            │  └─────────────────────────────┘   │
        │            │                                     │
        │            │  Business Services read:            │
        │            │  GET cp:state:{id} (Redis/Dynamo)   │
        │            │  PUBLISH cmd → WSGW → charger       │
        │            └──────────────┬──────────────────────┘
        │                           │
        │            ┌──────────────▼───────────┐
        │            │  Business Services        │
        │            │  (REST API layer)         │
        │            │  Charge Box Mgmt          │
        │            │  EVSE Control             │
        │            │  Remote Start/Stop        │
        │            │  Token Auth               │
        │            └──────────────┬────────────┘
        │                           │
        └──────────────┬────────────┘
                       │
        ┌──────────────▼─────────────────┐
        │         Downstream             │
        │  Billing    Telemetry   CDRs   │
        │  (RDS PG)  (Timestream) (S3)   │
        └────────────────────────────────┘
```

**Three independent data flows:**

**(A) Upstream — charger → backend (via Kafka):**
WSGW publishes OCPP events (MeterValues, StatusNotification, Transactions) to Kafka topics. Kafka consumers (Billing, Telemetry, Analytics) process asynchronously. Redis and DynamoDB are not involved.

**(B) Latest-state write — synchronous, before Kafka publish:**
On every OCPP message, WSGW enriches the event (adds `messageId`, `ingestedAt`, `sequenceNumber`) and writes the updated charger state to DynamoDB (source of truth) and Redis (read-through cache). This happens before the Kafka publish, ensuring latest-state is always current regardless of downstream lag.

**(C) Command routing — backend → charger (via Redis):**
Business Service looks up which WSGW pod holds the target charger's connection (`GET cp:{id}:pod`), then publishes a command to that pod's channel. The pod delivers it over the open WebSocket. Kafka is not involved.

Each charger establishes a single **outbound** persistent WebSocket connection to the CSMS endpoint:
```
wss://ocpp.spirii.com/ocpp/1.6/{chargePointId}
```
The `chargePointId` (typically the serial number) is provisioned into the charger firmware during on-site installation.

---

## 2. Network Load Balancer (NLB)

```
Listener:             TCP:443
Target Group:         ws-gateway pods
Protocol:             TCP (L4 passthrough)
Health Check:         HTTP /healthz (port 8080)
Deregistration Delay: 300s
Connection Draining:  Enabled
Stickiness:           Source IP hash
Cross-Zone LB:        Enabled
```

NLB provides a **static IP per AZ** — useful when intermediate network equipment on the charger site requires a fixed outbound destination.

---

## 3. WebSocket Gateway (WSGW)

### Responsibilities

- Accept and hold WebSocket connections from chargers
- Parse incoming OCPP JSON messages
- Publish events to Kafka (by message type)
- Receive commands from business services via Redis Pub/Sub and forward to the target charger

### Runtime: Go

~50,000 connections per pod at 2 GB RAM using goroutine-per-connection model.

### Pod internals

```
┌──────────────────────────────────────────────┐
│              WSGW Pod (Go)                   │
│                                              │
│  WebSocket Server                            │
│    in-memory: { chargePointId → *Conn }      │
│                                              │
│  On Connect:                                 │
│    Redis SET cp:{id}:pod  "ws-pod-2"  EX 300 │
│                                              │
│  On OCPP Message:                            │
│    → Kafka Producer → topic by message type  │
│                                              │
│  On Redis Pub/Sub message:                   │
│    → lookup conn by chargePointId            │
│    → send OCPP command over WebSocket        │
│                                              │
│  Prometheus metrics:                         │
│    ws_active_connections (gauge)             │
│    ocpp_messages_total (counter)             │
└──────────────────────────────────────────────┘
```

### K8s Deployment

```yaml
apiVersion: apps/v1
kind: Deployment
metadata:
  name: ws-gateway
spec:
  replicas: 8
  strategy:
    type: RollingUpdate
    rollingUpdate:
      maxUnavailable: 0
      maxSurge: 2
  template:
    metadata:
      labels:
        app: ws-gateway
    spec:
      tolerations:
      - key: dedicated
        value: ws-gateway
        effect: NoSchedule
      nodeSelector:
        nodegroup: ws-gateway
      terminationGracePeriodSeconds: 300
      topologySpreadConstraints:
      - maxSkew: 1
        topologyKey: topology.kubernetes.io/zone
        whenUnsatisfiable: DoNotSchedule
        labelSelector:
          matchLabels:
            app: ws-gateway
      - maxSkew: 1
        topologyKey: kubernetes.io/hostname
        whenUnsatisfiable: DoNotSchedule
        labelSelector:
          matchLabels:
            app: ws-gateway
      containers:
      - name: ws-gateway
        image: spirii/ws-gateway:latest
        resources:
          requests:
            memory: "1.5Gi"
            cpu: "1"
          limits:
            memory: "2Gi"
            cpu: "2"
        ports:
        - containerPort: 8080
        readinessProbe:
          httpGet:
            path: /healthz
            port: 8080
          initialDelaySeconds: 5
          periodSeconds: 10
        lifecycle:
          preStop:
            exec:
              command: ["/bin/sleep", "30"]
```

### HPA

```yaml
apiVersion: autoscaling/v2
kind: HorizontalPodAutoscaler
spec:
  scaleTargetRef:
    name: ws-gateway
  minReplicas: 8
  maxReplicas: 25
  metrics:
  - type: Pods
    pods:
      metric:
        name: ws_active_connections
      target:
        type: AverageValue
        averageValue: "8000"
  behavior:
    scaleUp:
      stabilizationWindowSeconds: 60
      policies:
      - type: Pods
        value: 2
        periodSeconds: 60
    scaleDown:
      stabilizationWindowSeconds: 600
      policies:
      - type: Pods
        value: 1
        periodSeconds: 120
```

### Node Group (EKS Managed)

```
Instance:      c6i.4xlarge (16 vCPU, 32 GB RAM, 12.5 Gbps)
Min nodes:     5
Desired:       6
Max nodes:     15
Purchase:      On-Demand only (stateful — no Spot)
Taint:         dedicated=ws-gateway:NoSchedule
Layout:        2 pods per node, spread across 3 AZ
```

#### Kernel tuning (Launch Template user data)

```bash
#!/bin/bash
cat >> /etc/sysctl.conf << EOF
net.core.somaxconn           = 65535
net.ipv4.tcp_max_syn_backlog = 65535
net.ipv4.ip_local_port_range = 1024 65535
fs.file-max                  = 2097152
net.ipv4.tcp_fin_timeout     = 15
net.ipv4.tcp_keepalive_time  = 60
net.ipv4.tcp_keepalive_intvl = 10
net.ipv4.tcp_keepalive_probes = 6
net.core.rmem_max            = 16777216
net.core.wmem_max            = 16777216
EOF
sysctl -p
```

### PodDisruptionBudget

```yaml
apiVersion: policy/v1
kind: PodDisruptionBudget
metadata:
  name: ws-gateway-pdb
spec:
  minAvailable: 6
  selector:
    matchLabels:
      app: ws-gateway
```

---

## 4. Redis Cluster — Connection Routing

### Problem

WSGW pods are stateful — each charger is connected to a specific pod. Business services need to send commands (RemoteStart, ChangeConfig, etc.) to a specific charger without knowing which pod holds its connection.

### Routing flow

```
Business Service: "send RemoteStart to CP-007"
        │
        ▼
  Redis GET cp:CP-007:pod  →  "ws-pod-3"
        │
        ▼
  Redis PUBLISH channel:ws-pod-3  {command payload}
        │
        ▼
  ws-pod-3 (subscribed to its own channel)
        │
        ▼
  lookup conn for CP-007 → send OCPP command via WebSocket
```

### Data model

```
# Updated on every Heartbeat (TTL = heartbeat interval + buffer)
SET  cp:{chargePointId}:pod    "ws-pod-3"          EX 300

# Command delivery channel per pod
SUBSCRIBE  channel:ws-pod-3

# Optional: charger metadata
HSET cp:{chargePointId}:meta
     status       "Charging"
     connectedAt  "2026-06-15T10:00:00Z"
     evseId       "1"
```

### ElastiCache configuration

```
Mode:           Cluster Mode Enabled
Shards:         3
Replicas:       1 per shard  →  6 nodes total
AZ layout:      One shard primary per AZ

  AZ 1a: Primary-1  ↔  AZ 1b: Replica-1
  AZ 1b: Primary-2  ↔  AZ 1c: Replica-2
  AZ 1c: Primary-3  ↔  AZ 1a: Replica-3

Instance:       cache.r7g.large (2 vCPU, 13 GB)
Failover:       Automatic (~30s on primary failure)
Encryption:     TLS in-transit + Auth token
```

### Behavior on WSGW pod failure

```
ws-pod-3 crashes
  │
  ├── Connected chargers disconnect
  │   → exponential backoff + jitter → reconnect to another pod
  │
  ├── Redis keys cp:*:pod="ws-pod-3" expire after TTL (300s)
  │   → or overwritten immediately when charger reconnects
  │
  └── Pending commands targeting "ws-pod-3":
      → Business Service gets Redis miss → retry queue
```

---

## 5. Kafka / MSK — Message Pipeline

### Topics

| Topic | Producer | Consumers | Retention |
|---|---|---|---|
| `ocpp.boot_notification` | WSGW | Auth, Registry | 3 days |
| `ocpp.status_notification` | WSGW | Alert Service, Analytics | 3 days |
| `ocpp.meter_values` | WSGW | Telemetry, Analytics | 7 days |
| `ocpp.transactions` | WSGW | Billing | 30 days |
| `ocpp.firmware_status` | WSGW | Device Management | 3 days |

### Traffic estimate

```
50k chargers × 1 MeterValues/min  =    833 msg/s
150k chargers × 1 MeterValues/min =  2,500 msg/s
All topics combined (peak):        ~  5,000 msg/s
```

### MSK configuration

```
Brokers:            3 (one per AZ)
Instance:           kafka.m5.2xlarge (8 vCPU, 32 GB)
Storage:            2 TB per broker (gp3, encrypted)
Replication factor: 3
min.insync.replicas: 2

Partitions:
  ocpp.meter_values:         36   (highest throughput)
  ocpp.transactions:         12
  ocpp.status_notification:  12
  others:                     6

Partition key: chargePointId
  → guaranteed ordering of events per charger
```

---


## 6. Latest-State Store

### Concept

At any moment, any service or API consumer must be able to ask:

```
GET /chargers/CP-007/state
```

and receive a current, accurate response — regardless of whether the charger is online, offline, charging, or idle. This is the **latest-state store**: a always-up-to-date snapshot of every charger, separate from historical telemetry and separate from routing state.

### Why a dedicated store

| Store | Suitable for latest-state? | Reason |
|---|---|---|
| Kafka | No | Stream, not queryable |
| Timestream | No | Historical aggregates, not point lookups |
| Redis (`cp:{id}:pod`) | No | Routing only, expires on disconnect |
| Redis (`cp:{id}:session:*`) | Partial | Active sessions only, volatile |
| **DynamoDB** | **Yes** | Persistent, < 10ms reads, queryable via GSI |
| Redis (cache layer) | Yes (cache only) | Fast reads, but volatile — needs DynamoDB as backing store |

### Architecture: DynamoDB + Redis read-through

```
WSGW receives OCPP message
  │
  ├── 1. Enrich event (messageId, ingestedAt, sequenceNumber)
  │
  ├── 2. Write latest-state → DynamoDB  (source of truth)
  │        ConditionExpression: ingestedAt > current.lastSeen
  │
  ├── 3. Write latest-state → Redis     (read-through cache, TTL 5 min)
  │        cp:state:{chargePointId}
  │
  └── 4. Publish to Kafka               (async, for history consumers)

API read path:
  GET /chargers/{id}/state
    → Redis GET cp:state:{id}
        hit  → return (< 1ms)
        miss → DynamoDB GetItem → populate Redis → return (< 10ms)
```

DynamoDB is the **source of truth**. Redis is a performance layer. If Redis loses data (restart, eviction), the next read repopulates from DynamoDB transparently.

### DynamoDB data model

**Table:** `charger-latest-state`

```
PK:  chargePointId  (String)        e.g. "CP-007"

Attributes:
  locationId          String
  evseId              String
  
  connectivity:
    status            String         "Online" | "Offline"
    lastSeen          ISO8601        server-side ingestedAt timestamp
    ocppVersion       String         "1.6" | "2.0.1"
    firmwareVersion   String
  
  ocppStatus          String         "Available" | "Charging" |
                                     "Faulted" | "Unavailable" | ...
  errorCode           String | null

  session:
    active            Boolean
    transactionId     Number | null
    startTime         ISO8601 | null
    tokenUid          String | null
    energyKwh         Number         running total this session
    powerW            Number         current power draw
    estimatedCostDKK  Number

  lastMeterValue:
    value             Number
    unit              String         "kWh"
    timestamp         ISO8601        ocppTimestamp from charger

  sequenceNumber      Number         monotonic per chargePointId
  messageId           String         UUID of last processed message
  updatedAt           ISO8601        ingestedAt of last update
```

**GSI examples** (for fleet-wide queries):

```
GSI: locationId-index
  PK: locationId
  → "give me all chargers at location loc-copenhagen-01"

GSI: ocppStatus-index
  PK: ocppStatus
  → "give me all Faulted chargers across the network"

GSI: connectivity.status-index
  PK: connectivity.status
  → "how many chargers are currently Offline?"
```

### Which OCPP messages update which fields

| OCPP Message | Fields updated |
|---|---|
| `BootNotification` | `connectivity.*`, `firmwareVersion`, `ocppVersion` |
| `Heartbeat` | `connectivity.lastSeen`, `connectivity.status = Online` |
| `StatusNotification` | `ocppStatus`, `errorCode` |
| `MeterValues` | `session.energyKwh`, `session.powerW`, `lastMeterValue.*` |
| `StartTransaction` | `session.active = true`, `session.transactionId`, `session.startTime`, `session.tokenUid` |
| `StopTransaction` | `session.active = false`, clears `session.*` fields |
| WebSocket disconnect | `connectivity.status = Offline` (set by WSGW on connection close) |

### Offline detection

When a WebSocket connection closes (charger disconnect, network loss), WSGW immediately writes:

```json
{
  "connectivity.status": "Offline",
  "connectivity.lastSeen": "<now>"
}
```

This means `lastSeen` always reflects the last moment the charger was reachable, and `status = Offline` is visible instantly — without waiting for a Heartbeat timeout.

---

## 7. Idempotency & Out-of-Order Event Handling

### Problem scenarios

**Duplicate events (Kafka at-least-once delivery):**
```
MeterValues message delivered twice to Timestream consumer
→ two identical records at same timestamp
→ inflated energy totals in reports

StopTransaction delivered twice to Transaction Processor
→ two CDRs created for one session
→ customer billed twice
```

**Out-of-order events (charger buffering during offline period):**
```
Charger offline 3 minutes, buffers events locally
→ reconnects, flushes buffer: t=14:00, t=14:01, t=14:02
→ but Kafka already has t=14:05 (sent before disconnect)
→ latest-state updated with t=14:02 → shows stale power reading
```

**Late StopTransaction:**
```
StopTransaction for transactionId=456 arrives after
StartTransaction for transactionId=457 already opened
→ naive handler overwrites active session state with closed session
→ charger appears idle when it is actually charging
```

### Solution layer 1 — Event enrichment at WSGW

Every OCPP message is enriched before any downstream write:

```
ingestedAt:      server-side UTC timestamp (monotonic, not charger clock)
ocppTimestamp:   original timestamp from charger (used for history)
messageId:       UUIDv4 — globally unique per message
sequenceNumber:  per-chargePointId counter, atomically incremented in Redis
                 INCR cp:seq:{chargePointId}
```

`ingestedAt` is the authority for latest-state ordering.
`ocppTimestamp` is the authority for historical time-series placement.
`messageId` is the idempotency key for all downstream consumers.

### Solution layer 2 — Latest-state: optimistic locking

DynamoDB conditional write prevents out-of-order state corruption:

```python
dynamodb.update_item(
    Key={"chargePointId": "CP-007"},
    UpdateExpression="SET ocppStatus = :s, updatedAt = :t, ...",
    ConditionExpression="attribute_not_exists(updatedAt) "
                        "OR updatedAt < :t",
    ExpressionAttributeValues={
        ":s": "Charging",
        ":t": event.ingestedAt   # server-side timestamp
    }
)
```

If a late event arrives with an older `ingestedAt`, the condition fails silently — the stale event is ignored for state, but still published to Kafka for history completeness.

### Solution layer 3 — History: idempotency table

For consumers that must not process a message twice (Transaction Processor, CDR generator):

```
DynamoDB table: processed-events
  PK:  messageId   (UUIDv4)
  TTL: 7 days      (longer than Kafka retention)
  
Before processing any message:
  1. Attempt conditional write:
     PutItem with ConditionExpression: attribute_not_exists(messageId)
  2. Success → process the message
  3. ConditionalCheckFailedException → duplicate, skip silently
  
Cost: ~500 bytes × 50,000 events/day = ~25 MB/day → negligible
```

### Solution layer 4 — Timestream: native out-of-order support

Timestream accepts writes with any timestamp in the `time` field. Records are stored and queried in chronological order regardless of ingestion order.

```
Write with:  time = event.ocppTimestamp  (charger clock)

Late event t=14:02 arriving after t=14:05 is accepted and
inserted at the correct position in the time series.

Limitation: Timestream rejects records older than the
memory store retention window (configurable, default 24h).
For chargers offline > 24h, buffered events must be written
directly to the magnetic store tier via batch load.
```

### Out-of-order handling summary

```
Event arrives at WSGW
  │
  ├── Enrich: messageId + ingestedAt + sequenceNumber
  │
  ├── Latest-state write (DynamoDB)
  │     ConditionExpression: updatedAt < ingestedAt
  │     → stale events silently rejected
  │     → only newest event wins
  │
  ├── Kafka publish (always — even stale events go to history)
  │
  └── Downstream consumers:
        Timestream:           write with ocppTimestamp → correct ordering
        Transaction Processor: check messageId in processed-events table
        CDR generator:         check messageId in processed-events table
```

### Late StopTransaction protection

Transaction Processor checks session continuity before writing CDR:

```python
def handle_stop_transaction(event):
    state = dynamodb.get(event.chargePointId)
    
    # Guard: only close the session this StopTransaction belongs to
    if state.session.transactionId != event.transactionId:
        # Late stop for an already-superseded transaction
        # Write CDR to S3 archive but do not update latest-state
        write_cdr_archive(event)
        return
    
    # Normal path: close session, generate CDR
    generate_cdr(event, state)
    update_latest_state(event.chargePointId, session_active=False)
```

## 8. Capacity Planning

### Resource matrix

| Component | 50k CP | 100k CP | 150k CP |
|---|---|---|---|
| WSGW Pods | 8 | 14 | 20 |
| Worker Nodes (c6i.4xlarge) | 5 | 8 | 12 |
| Redis shards | 3 | 3 | 6 |
| MSK brokers | 3 | 3 | 3 |
| MSK partitions (meter_values) | 36 | 36 | 72 |

### WSGW pod memory breakdown

```
TCP socket buffers (kernel):  ~12 KB / connection
Application state (Go):        ~8 KB / connection
─────────────────────────────────────────────────
Total:                        ~20 KB / connection

Pod limit 2 GB → theoretical max: ~100,000 connections
Operating target:               8,000 connections/pod
Safety headroom:                  6x
```

### Thundering herd protection

```
Charger firmware:  exponential backoff + jitter
  delay = min(5 × 2^attempt, 300s) + random(0, 30s)

WSGW:  max new connections rate-limited to 500/s
NLB:   connection draining 300s on pod deregistration
```

---

## 9. Disaster Recovery & Failover Scenarios

### Scenario 1 — WSGW Pod failure

```
Trigger:   OOM kill, crash, eviction

t=0s       Pod goes down
t=0–30s    K8s detects via liveness probe; replacement pod starts
t=30s      NLB health check removes pod from target group
t=30–300s  Chargers reconnect (backoff + jitter)
t=~5min    New pod has absorbed connections

Impact:    ~1/8 of chargers temporarily disconnected (30–300s)
RTO:       < 5 minutes
```

### Scenario 2 — Worker Node failure

```
Trigger:   EC2 instance failure

t=0s       Node unreachable
t=~1min    K8s marks node NotReady; pods evicted
t=~3min    Replacement pods scheduled on remaining nodes
t=~5min    Cluster Autoscaler provisions new node
t=~7min    New node Ready; pods scheduled

Protected by:
  PodDisruptionBudget:        minAvailable: 6
  topologySpreadConstraints:  max 2 pods per node

Impact:    ~2/8 pods temporarily down (3–7 min)
RTO:       < 10 minutes
```

### Scenario 3 — Full AZ failure

```
Trigger:   AWS AZ outage (e.g., eu-west-1b)

t=0s       AZ 1b unavailable
t=~30s     NLB health checks fail for 1b targets
           → traffic redirected to 1a and 1c automatically
t=~2min    HPA detects increased connections/pod → scale up
           Cluster Autoscaler adds nodes in 1a and 1c
t=~5min    New pods Ready; load rebalanced

Capacity during outage:
  2 AZ × ~3 pods = 6 pods → 75% capacity
  HPA compensates within ~5 min

Impact:    ~1/3 chargers reconnect; ~5 min degraded capacity
RTO:       < 10 minutes
```

### Scenario 4 — Redis shard failure

```
Trigger:   Redis primary node failure

t=0s       Primary unavailable
t=~30s     ElastiCache automatic failover to replica

During failover (30s):
  - New connections: Redis SET retried → no data loss
  - Commands to chargers: retry queue in Business Service
  - Active connections: unaffected (WSGW holds connections in memory)

RTO:       < 1 minute
```

### Scenario 5 — MSK broker failure

```
Trigger:   One Kafka broker unavailable

Impact:    None — RF=3, min.insync.replicas=2
           Two remaining brokers continue serving reads and writes.
           MSK rebalances partition leaders automatically.

RTO:       0 (transparent to producers and consumers)
```

### DR Summary

| Scenario | RTO | RPO | Automatic |
|---|---|---|---|
| Pod failure | < 5 min | 0 | Yes (K8s) |
| Node failure | < 10 min | 0 | Yes (K8s + CA) |
| AZ failure | < 10 min | 0 | Yes (NLB + HPA) |
| Redis shard failure | < 1 min | 0 | Yes (ElastiCache) |
| MSK broker failure | 0 | 0 | Yes (Kafka RF=3) |
| Full region failure | Manual | Depends on DR tier | No |

---

## 10. Charger Authentication & mTLS

### Threat model

Without strong charger authentication, any client knowing the WebSocket URL can connect and inject fake OCPP messages — fake transactions, false meter values, or denial-of-service floods.

### Authentication options

| Method | How it works | Strength | Notes |
|---|---|---|---|
| One-way TLS | Server cert only; charger identified by `chargePointId` in URL | Low | Anyone knowing the URL can connect |
| Basic Auth over TLS | `Authorization: Basic` header in WebSocket upgrade | Medium | OCPP 1.6 supported; password must be stored on charger |
| **mTLS** | Both sides present certificates; CSMS verifies client cert against CA | **High** | OCPP 2.0.1 recommended; requires PKI infrastructure |

### mTLS architecture

```
┌──────────────────────────────────────────────┐
│           PKI Infrastructure                 │
│                                              │
│  AWS Private CA (ACM PCA)                   │
│  └── Root CA (offline, HSM-backed)          │
│      └── Intermediate CA (online)           │
│          ├── Server cert  → WSGW pods       │
│          └── Client certs → each charger    │
└──────────────────────────────────────────────┘
```

### TLS termination

NLB operates in **TCP passthrough** mode — it does not terminate TLS. TLS is terminated at the WSGW pod, which has access to the CA trust store for client certificate verification.

```
Charger
  │  TLS ClientHello + client certificate
  ▼
NLB  (TCP passthrough — raw TLS forwarded)
  │
  ▼
WSGW Pod
  ├── Terminates TLS
  ├── Verifies client cert against ACM PCA trust store
  ├── Extracts chargePointId from cert CN or SAN field
  └── Upgrades to WebSocket → OCPP session authenticated
```

### Certificate provisioning

**At installation time:**
```
1. Installer triggers CSR generation on the charger
2. CSR sent to Spirii Provisioning API
3. ACM PCA signs the certificate
4. Signed cert returned and stored in charger secure storage
5. Charger private key never leaves the device
```

**Via OCPP 2.0.1 (for supported hardware):**
```
CSMS → InstallCertificate.req  → charger installs CA cert
Charger → SignCertificate.req  → sends CSR to CSMS
CSMS → ACM PCA                 → signs CSR
CSMS → CertificateSigned.req   → charger installs client cert
```

### Certificate rotation

```
ACM PCA monitors expiry
  → 30 days before expiry:
      CSMS initiates rotation via OCPP CertificateManagement
      or out-of-band provisioning API
  → Charger installs new cert alongside existing one
  → On next reconnect: new cert presented
  → Old cert revoked in ACM PCA CRL / OCSP
```

### Compatibility note

mTLS requires charger firmware support for client certificates. Legacy hardware (OCPP 1.6 only, older firmware) may support only Basic Auth. A hybrid approach is acceptable during migration:
- New chargers (OCPP 2.0.1): mTLS
- Legacy chargers (OCPP 1.6): Basic Auth over TLS + IP allowlisting where possible

---

## 11. Key Design Decisions

| Decision | Choice | Rationale |
|---|---|---|
| Load Balancer | NLB (L4 TCP) | TCP passthrough preserves WebSocket connections across pod restarts and enables mTLS passthrough to pods |
| TLS termination | WSGW pod (not NLB) | Required for mTLS client certificate verification at application layer |
| Charger authentication | mTLS (OCPP 2.0.1) + Basic Auth fallback (OCPP 1.6) | Cryptographic identity per charger; fallback for legacy hardware |
| CA infrastructure | AWS Private CA (ACM PCA) | Managed PKI with CRL/OCSP, integrates with ACM and IAM |
| WSGW runtime | Go | Goroutine model: ~50k connections per pod at 2 GB RAM |
| Node purchase type | On-Demand only | Spot termination causes thundering herd from thousands of chargers |
| Node Group isolation | Dedicated taint | Prevents other workloads from competing for RAM and file descriptors |
| HPA metric | `ws_active_connections` | CPU/memory do not reflect connection count accurately |
| HPA scale-down window | 600s | Prevents rapid pod removal and thundering herd |
| Kafka partition key | `chargePointId` | Guarantees per-charger event ordering for billing and telemetry |
| Redis TTL | 300s (= heartbeat interval × 4) | Keys expire automatically if charger disconnects without cleanup |

---

## 12. Telemetry & Data Strategy

This architecture covers the base infrastructure layer (WSGW, Kafka, Redis). The sections below describe how telemetry data flows downstream and what storage tier serves each use case.

### Use cases and data requirements

| Use Case | Consumer | Latency | Granularity | Retention |
|---|---|---|---|---|
| Network health monitoring | NOC team | < 30s | Per event | 7 days |
| Live session progress | Driver app | < 5s | Per event | Session only |
| Billing & invoicing | Billing service | After session | Session total | 7 years (EU) |
| Location utilization reports | Location owners | Daily | Hourly aggregate | 2 years |
| Smart charging / grid mgmt | Energy service | < 1 min | Per minute | 30 days |
| ESG / sustainability reporting | Management | Monthly | Daily aggregate | 5 years |
| Predictive maintenance | Analytics | Offline | Per event | 3 years |

Different use cases require fundamentally different latency, granularity, and retention characteristics. A single storage layer cannot serve all of them efficiently.

### Charge Detail Record (CDR)

At session end, a **CDR** (Charge Detail Record) is produced — a single immutable JSON document summarising the complete charging session. This is the canonical billing artifact and the standard unit of exchange in OCPI roaming.

```json
{
  "cdr_id": "cdr-2026-06-15-CP007-456",
  "chargePointId": "CP-007",
  "locationId": "loc-copenhagen-downtown",
  "session": {
    "startTime": "2026-06-15T14:00:00Z",
    "stopTime":  "2026-06-15T16:30:00Z",
    "durationMinutes": 150,
    "stopReason": "EVDisconnected"
  },
  "energy": {
    "totalKwh": 45.2,
    "startMeterValue": 1234.5,
    "stopMeterValue":  1279.7
  },
  "billing": {
    "tariffId": "tariff-dk-standard",
    "currency": "DKK",
    "totalCost": 112.50,
    "breakdown": {
      "energyCost": 90.40,
      "sessionFee": 5.00,
      "parkingFee": 17.10
    }
  },
  "auth": {
    "tokenType": "RFID",
    "tokenUid": "04:AB:CD:EF:12:34",
    "customerId": "cust-789"
  },
  "roaming": {
    "emspId": "DK-MOB",
    "ocpiSessionId": "ocpi-sess-999"
  }
}
```

**Important:** billing does not require raw MeterValues. The OCPP `StopTransaction` message contains `meterStop` — the final meter reading. `totalKwh = meterStop − meterStart`. Raw MeterValues are only needed for live session display and anomaly detection during the session.

### Storage tiers

| Data | Storage | Retention | Use |
|---|---|---|---|
| Raw MeterValues | Kafka only | 7 days | Transport — not archived |
| Live session state | Redis (TTL) | Session duration | Driver app, live cost estimate |
| 1-min aggregates | Timestream | 90 days | NOC dashboards, grid management |
| CDRs (active) | RDS PostgreSQL | 2 years | Billing queries, disputes |
| CDRs (archive) | S3 (JSON/Parquet) | 7 years | EU legal requirement |
| Daily/hourly aggregates | S3 + Athena | 5 years | Reports, ESG, planning |

### Telemetry pipeline

```
Kafka: ocpp.meter_values
  │
  ├──► Stream Processor (Kinesis Data Analytics / Flink)
  │         │
  │         ├── 1-min windowed aggregates ──► Timestream (90 days)
  │         │   { chargePointId, avg_power, max_power,
  │         │     energy_delta, timestamp_bin }
  │         │
  │         ├── anomaly detection ──► SNS → PagerDuty / NOC
  │         │   (no meter values >5 min, power spike, stuck session)
  │         │
  │         └── live session state ──► Redis (TTL = session duration)
  │             cp:{id}:session:energy_so_far
  │             cp:{id}:session:cost_so_far
  │
Kafka: ocpp.transactions
  │
  └──► Transaction Processor (Lambda / ECS)
            │
            On StopTransaction:
            ├── fetch tariff from RDS
            ├── calculate CDR
            ├── write CDR → RDS PostgreSQL
            ├── write CDR → S3 (long-term archive)
            ├── clear Redis session state
            └── if roaming → publish to OCPI service
```

### Downstream services (application layer)

This base infrastructure supports the following application-layer services, each deployed independently on EKS or ECS:

| Service | Kafka topics consumed | Storage | Function |
|---|---|---|---|
| Stream Processor | meter_values, status | Timestream, Redis, SNS | Real-time aggregation, anomaly detection |
| Transaction Processor | transactions | RDS, S3 | CDR creation, billing, tariff calculation |
| REST API | — | RDS, Redis | CPO management (locations, EVSEs, tokens) |
| Auth Service | boot_notification | RDS, Redis | RFID/token validation |
| OCPI Service | transactions | RDS | Roaming CDR exchange |
| Reporting Service | — | S3 + Athena | Dashboards, ESG, sustainability reports |
| Alert Service | status_notification | — | NOC alerting via SNS / PagerDuty |

---

## 13. Cost Estimate

All prices are AWS eu-west-1 on-demand rates as of June 2026. Reserved Instance or Savings Plans pricing reduces compute costs by approximately 30–40%.

### Base infrastructure (WSGW + NLB + MSK + Redis + PKI)

| Component | Config | 50k CP | 100k CP | 150k CP |
|---|---|---|---|---|
| EKS Control Plane | — | $150 | $150 | $150 |
| EC2 WSGW nodes | c6i.4xlarge, On-Demand | $3,000 | $5,500 | $8,000 |
| Network Load Balancer | multi-AZ + LCU | $150 | $200 | $250 |
| Amazon MSK | m5.2xlarge ×3 | $1,500 | $1,500 | $2,500 |
| ElastiCache (Redis) | r7g.large ×6 | $900 | $900 | $1,500 |
| ACM Private CA | intermediate CA | $400 | $400 | $400 |
| **Base total** | | **~$6,100** | **~$8,650** | **~$12,800** |

### Application layer (downstream services + storage)

| Component | Config | 50k CP | 100k CP | 150k CP |
|---|---|---|---|---|
| Kinesis Data Analytics | stream processor | $400 | $700 | $1,000 |
| Amazon Timestream | writes + storage | $300 | $550 | $800 |
| RDS PostgreSQL | r6g.2xlarge Multi-AZ | $800 | $800 | $1,500 |
| S3 (CDRs + data lake) | standard + Glacier | $100 | $180 | $260 |
| Athena | ad-hoc queries | $50 | $80 | $120 |
| Lambda / ECS tasks | transaction processor | $50 | $80 | $120 |
| **App layer total** | | **~$1,700** | **~$2,390** | **~$3,800** |

### Total monthly estimate

| Scale | Base infra | App layer | **Total/month** | **Total/year** |
|---|---|---|---|---|
| 50k chargers | $6,100 | $1,700 | **~$7,800** | **~$94k** |
| 100k chargers | $8,650 | $2,390 | **~$11,000** | **~$132k** |
| 150k chargers | $12,800 | $3,800 | **~$16,600** | **~$199k** |

### Cost optimisation levers

- **Savings Plans (1-year compute):** reduces EC2 and Fargate costs by ~30% → saves ~$1,000–2,500/month at scale
- **MSK tiered storage:** offload older Kafka segments to S3 at ~$0.023/GB vs $0.10/GB on broker EBS
- **Timestream automatic tiering:** hot → magnetic → cold automatically; no action required
- **S3 Intelligent-Tiering:** for CDR archive — automatically moves infrequently accessed objects to cheaper storage tiers
- **Spot instances for stateless consumers:** Stream Processor, Reporting, ETL jobs can use Spot (~70% savings on those workloads)

---

## 14. Open Issues & Out-of-Scope Topics

The following items are relevant to a production deployment but are not covered in this document.

### Multi-region strategy

The current architecture is single-region (`eu-west-1`). A full AWS region outage would result in complete platform unavailability. A minimum viable DR approach would be an **active-passive standby** in `eu-central-1` with Route53 health check failover.

Key challenges for multi-region WebSocket platforms:
- Chargers maintain persistent connections — cross-region session handoff is non-trivial
- Redis connection registry is regional — a failover region starts with no connection state
- Kafka replication lag means some in-flight events may be lost during failover

This requires a dedicated architecture decision and RPO/RTO budget agreement before implementation.

### Redis command delivery reliability

When a Business Service sends a command to a charger (e.g. RemoteStart), it publishes to a Redis Pub/Sub channel. If Redis is unavailable (e.g. during the ~30s primary failover window), the command is lost.

The retry mechanism needs to be explicitly designed. Recommended approach: **SQS queue with Dead Letter Queue (DLQ)** as a durable buffer for outbound commands, with WSGW draining the queue per charger connection. This decouples command durability from Redis availability.

### Observability strategy

Not covered: distributed tracing (AWS X-Ray or OpenTelemetry), structured log aggregation strategy (CloudWatch Log Groups per service), business-level SLO definitions (e.g. session start success rate, authorization latency p99), and alerting runbooks.

---

*Version 1.3 — June 2026*
