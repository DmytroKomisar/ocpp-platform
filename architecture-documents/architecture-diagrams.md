# OCPP Platform — Architecture Diagrams

> All diagrams use [Mermaid](https://mermaid.js.org/) and render automatically in GitHub.

---

## 1. High-Level Architecture

```mermaid
graph TD
    CP1[EV Charger] & CP2[EV Charger] & CP3[EV Charger ···50k–150k]
    -->|WSS / TLS 1.3 outbound| R53

    R53[Route53\nLatency-based routing]
    --> NLB

    NLB[Network Load Balancer\nTCP passthrough · static IP per AZ · mTLS]
    --> WSGW_A & WSGW_B & WSGW_C

    subgraph EKS ["EKS Cluster — eu-west-1"]
        WSGW_A[WSGW pods\nAZ 1a]
        WSGW_B[WSGW pods\nAZ 1b]
        WSGW_C[WSGW pods\nAZ 1c]
    end

    WSGW_A & WSGW_B & WSGW_C -->|A · events| MSK
    WSGW_A & WSGW_B & WSGW_C -->|B · state write sync| DDB
    WSGW_A & WSGW_B & WSGW_C -->|C · routing| REDIS

    DDB[(DynamoDB\nLatest-state\nsource of truth)]
    -->|read-through| REDIS

    REDIS[(Redis ElastiCache\nRouting · session cache)]

    MSK[Amazon MSK — Kafka\n3 brokers · 3 AZ · RF=3]
    --> BIZ

    REDIS -->|pub/sub commands| BIZ
    DDB -->|state reads| BIZ

    BIZ[Business Services\nREST API layer]
    --> BILLING & TELEMETRY & ANALYTICS

    BILLING[Billing\nCDR · RDS PostgreSQL]
    TELEMETRY[Telemetry\nTimestream · S3]
    ANALYTICS[Analytics\nAthena · Redshift]

    style EKS fill:none,stroke-dasharray:5 5
```

---

## 2. Data Flow — Three Independent Paths

```mermaid
sequenceDiagram
    participant CP as EV Charger
    participant WSGW as WSGW Pod
    participant DDB as DynamoDB
    participant REDIS as Redis
    participant KAFKA as Kafka MSK
    participant BIZ as Business Service

    Note over CP,BIZ: Path A — Upstream telemetry (charger → Kafka)
    CP->>WSGW: OCPP message (MeterValues / Status / Transaction)
    WSGW->>WSGW: Enrich: messageId · ingestedAt · sequenceNumber
    WSGW->>KAFKA: Publish to topic (async)
    Note over KAFKA: Consumers: Billing, Telemetry, Analytics

    Note over CP,BIZ: Path B — Latest-state write (sync, before Kafka)
    WSGW->>DDB: Update latest-state\nCondition: ingestedAt > current.lastSeen
    WSGW->>REDIS: SET cp:state:{id} TTL=300s

    Note over CP,BIZ: Path C — Command routing (backend → charger)
    BIZ->>REDIS: GET cp:{id}:pod → "ws-pod-3"
    BIZ->>REDIS: PUBLISH channel:ws-pod-3 {command}
    REDIS->>WSGW: Deliver to ws-pod-3
    WSGW->>CP: OCPP command over WebSocket
```

---

## 3. WSGW — K8s Topology (Multi-AZ)

```mermaid
graph TD
    NLB[NLB\nTCP · source IP hash]

    subgraph AZ1 ["AZ eu-west-1a"]
        N1[Worker Node\nc6i.4xlarge]
        P1[ws-pod-1\n~8k connections]
        P2[ws-pod-2\n~8k connections]
        N1 --> P1 & P2
    end

    subgraph AZ2 ["AZ eu-west-1b"]
        N2[Worker Node\nc6i.4xlarge]
        P3[ws-pod-3\n~8k connections]
        P4[ws-pod-4\n~8k connections]
        N2 --> P3 & P4
    end

    subgraph AZ3 ["AZ eu-west-1c"]
        N3[Worker Node\nc6i.4xlarge]
        P5[ws-pod-5\n~8k connections]
        P6[ws-pod-6 buffer]
        N3 --> P5 & P6
    end

    NLB --> P1 & P2 & P3 & P4 & P5

    subgraph SHARED ["Shared layer — all AZ"]
        REDIS2[(Redis Cluster\n3 shards · 6 nodes)]
        KAFKA2[MSK Kafka\n3 brokers]
    end

    P1 & P2 & P3 & P4 & P5 --> REDIS2
    P1 & P2 & P3 & P4 & P5 --> KAFKA2

    HPA[HPA\nscale on ws_active_connections\nmin 8 · max 25 pods]
    CA[Cluster Autoscaler\nmin 5 · max 15 nodes]

    HPA -.->|controls| P1
    CA -.->|controls| N1

    style AZ1 fill:none,stroke-dasharray:5 5
    style AZ2 fill:none,stroke-dasharray:5 5
    style AZ3 fill:none,stroke-dasharray:5 5
    style SHARED fill:none,stroke-dasharray:3 3
```

---

## 4. Latest-State — Write & Read Path

```mermaid
flowchart TD
    MSG([OCPP message arrives at WSGW])
    ENRICH[Enrich: messageId · ingestedAt · sequenceNumber]

    MSG --> ENRICH
    ENRICH --> COND{ingestedAt newer than current lastSeen?}

    COND -->|Yes| DDB_W[Write to DynamoDB
Condition: updatedAt is older]
    COND -->|No - stale| KAFKA_ONLY[Publish to Kafka only
for history completeness]

    DDB_W --> REDIS_W[Write to Redis
cp-state-id, TTL 300s]
    DDB_W --> KAFKA_W[Publish to Kafka
async]

    REDIS_W --> DONE([State updated])
    KAFKA_W --> DONE

    subgraph READ ["API read path: GET /chargers/id/state"]
        API([API request])
        --> RC{Redis hit?}
        RC -->|Yes, under 1ms| RETURN_R([Return from Redis])
        RC -->|No, cache miss| DDB_R[Read from DynamoDB
under 10ms]
        DDB_R --> POPULATE[Populate Redis
TTL 300s]
        POPULATE --> RETURN_D([Return response])
    end

    subgraph IDEMPOTENCY ["Idempotency: Transaction Processor"]
        EVT([StopTransaction event])
        --> CHECK{messageId already in
processed-events table?}
        CHECK -->|Yes - duplicate| SKIP([Skip silently])
        CHECK -->|No - new| PROC[Process event
create CDR]
        PROC --> MARK[Write messageId
TTL 7 days]
        MARK --> CDR([CDR saved to
RDS and S3])
    end
```

---

## How to use in GitHub

Paste any diagram block directly into a `.md` file:

~~~markdown
```mermaid
graph TD
    A --> B
```
~~~

GitHub renders it automatically — no plugins or extensions needed.
