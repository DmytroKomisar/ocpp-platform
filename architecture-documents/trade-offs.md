# OCPP Telemetry Platform — Trade-offs & Assumptions

> **Companion to:** Architecture, Platform & Developer Experience, Operations
> **Focus:** What I assumed, what I deliberately skipped, what breaks first at 10x, and what I'd build next. Reasoning over completeness.

---

## Table of Contents

1. [Assumptions](#1-assumptions)
2. [Deliberately out of scope](#2-deliberately-out-of-scope)
3. [What breaks first at 10x](#3-what-breaks-first-at-10x)
4. [What I'd build next](#4-what-id-build-next)
5. [Conscious trade-offs — recap](#5-conscious-trade-offs--recap)

---

## 1. Assumptions

These shaped the design. If any is wrong, parts of the design change.

```
Charger behaviour
  - Firmware implements exponential backoff + jitter on reconnect.
    THIS IS load-bearing: without it, every disconnect is a thundering
    herd and the whole HA story weakens.
  - Chargers buffer telemetry locally when offline and flush on reconnect.
  - MeterValues sample roughly once per minute during a session.

Protocol scope
  - OCPP 1.6J and 2.0.1 are the versions in scope; 2.1 is a future adapter.
  - Authentication is OCPP Security Profile 2 (legacy) or 3 (new installs).

Platform scope
  - The mandate is "ingest telemetry + expose latest state + let teams build
    on it." Billing tariff logic, the driver app, and CPO dashboards are
    *consumers*, not part of this platform.
  - Multi-tenancy is pooled; no single tenant initially demands hard
    physical isolation or data residency.

Operational context
  - Single AWS region (eu-west-1) is acceptable for v1.
  - The Cloud team has the capacity to operate managed services and would
    rather trade money for reduced ops than run everything itself.
```

---

## 2. Deliberately out of scope

The brief invited scoping. These were left out on purpose, with reasoning — not forgotten.

| Skipped | Why | Cost of skipping |
|---|---|---|
| **Multi-region / cross-region DR** | Large effort; needs an explicit RPO/RTO budget and a cross-region WebSocket failover design. Single-region is a defensible v1. | A full region outage is total downtime. Flagged as the #1 open issue. |
| **Smart charging / V2G / load management** | A separate problem domain. OCPP supports it, but it's not "ingest telemetry + latest state." | None for this mandate; it's a future product on top. |
| **Billing / tariff engine internals** | A downstream consumer of CDRs. The platform produces correct CDRs; pricing logic is the billing team's domain. | None — clean ownership boundary. |
| **OCPI roaming design** | Acknowledged as a consumer of transactions; designing the roaming exchange is its own effort. | Roaming partners can't be integrated until built. |
| **Firmware OTA infrastructure** | Named (S3 + CloudFront), not detailed. Orthogonal to telemetry ingestion. | OTA rollout strategy still needs designing. |
| **Full observability topology** | Named the tools (X-Ray/OTel, CloudWatch); didn't design the trace graph. | Operability gap until specified. |
| **ISO 15118 / Plug & Charge depth** | Sits between EV and charger, below OCPP. Affects auth UX, not ingestion. | Plug & Charge UX not addressed. |

The pattern: I kept everything on the **ingestion-to-latest-state critical path and the platform layer**, and deferred adjacent domains that have clean boundaries with it.

---

## 3. What breaks first at 10x

Taking 50k → 500k chargers (or 150k → 1.5M). The failures are ordered by which I expect to hurt *first*, with reasoning — because they don't all break at once.

### First — reconnect storms get disproportionately worse

This is the earliest acute failure, and it's super-linear. At 50k chargers, an AZ failure means ~17k chargers reconnecting. At 500k, it's ~170k simultaneous reconnects. The backoff+jitter that comfortably absorbs 17k may not absorb 170k against a cluster that's also down an AZ.

```
The thundering herd doesn't scale linearly — it scales with the blast radius,
and the blast radius grows with fleet size. The deploy technique and AZ-failure
recovery that work at 50k need active reconnect-rate limiting at 500k:
a connection-admission gate at WSGW that paces accepts, so the herd is
metered in rather than allowed to stampede.
```

### Second — Redis becomes the routing/heartbeat hotspot

Every heartbeat writes to Redis (routing registry + lastSeen). At 500k chargers that's ~8,300 writes/s *just for keepalives*, before state and command traffic. The single Redis cluster's write throughput and Pub/Sub fanout become the steady-state bottleneck.

```
Fix: shard the routing registry by chargePointId hash across multiple Redis
clusters, or move routing to a purpose-built registry. Decouple the
heartbeat-write rate from the command-routing path so keepalives don't
compete with command delivery.
```

### Third — DynamoDB hot partitions for large tenants

The PK is `{tenantId}#{chargePointId}`, which spreads well *across* chargers — but a single very large CPO tenant concentrates write traffic, and the synchronous latest-state write sits on the WSGW ingestion hot path. At 10x, that inline write latency can become the ingestion ceiling.

```
Fix options (a trade-off in itself):
  - Add a write-sharding suffix for hot tenants.
  - OR move latest-state updates OFF the hot path: WSGW publishes to Kafka,
    a dedicated consumer updates DynamoDB. This removes the inline write
    but weakens the "latest-state is always instantly current" guarantee
    by the consumer's lag. Acceptable for most reads; a conscious trade.
```

### Fourth — single-region risk becomes unacceptable

At 500k chargers spanning Europe, concentrating everything in eu-west-1 is both a latency and a risk problem. What was a defensible v1 simplification becomes the thing that has to change.

Kafka, MSK, and the stateless plane scale fine to 10x with bigger/more nodes — they are **not** what breaks first. The pain is concentrated in the stateful connection layer and the synchronous state-write path, exactly where the design made its speed-vs-simplicity bets.

---

## 4. What I'd build next

In priority order, driven by the failure analysis above:

```
1. Multi-region active-passive.
   The single biggest reliability gap. Route53 health-check failover to a
   warm standby in eu-central-1. Hardest part is connection + latest-state
   state in the failover region — needs DynamoDB global tables and a
   reconnect-on-failover story.

2. Reconnect-storm hardening.
   Since it's what breaks first at scale: a connection-admission gate at WSGW
   that paces accepts, plus a coordinated, rate-limited recovery after AZ loss.
   Make the herd a queue.

3. Optional async latest-state path.
   Offer the Kafka-consumer-updates-DynamoDB model as an alternative to the
   inline write, for deployments where ingestion throughput matters more than
   sub-second state freshness. Let the trade be a config choice.

4. Finish building the platform layer.
   The schema registry, compatibility CI, paved-road Terraform modules, and
   Backstage templates are designed but need to actually be built and battle-
   tested. This is what turns the architecture into a platform teams self-serve.

5. FinOps controls.
   At 10x, cost needs active management: per-tenant cost attribution, Timestream
   and Kafka spend alerts, Savings Plans coverage targets.
```

---

## 5. Conscious trade-offs — recap

The decisions I'd defend in the interview, each with what it buys and what it costs:

| Decision | Buys | Costs |
|---|---|---|
| AWS managed services (DynamoDB, KDA, Timestream) | Low ops burden — Cloud team isn't the bottleneck | High lock-in on three components (exit path documented) |
| Single region for v1 | Simplicity, speed to ship | Total outage on region failure — deferred, not solved |
| Synchronous latest-state write on hot path | Latest-state is always instantly current | Becomes an ingestion ceiling at 10x; async path is the escape |
| Pooled multi-tenancy | One stack to operate, not N | Isolation depends on infra correctness (RLS, PK) — enforced, but a real attack surface |
| Go for WSGW | ~50k connections/pod, cheap | A less common language in the team than, say, Java |
| Canonical event schema + envelope/payload split | Version-agnostic downstream; clean ownership line | An abstraction layer to maintain; new OCPP features need adapter work |
| Archive raw MeterValues to S3 | Audit, ML, dispute capability for ~$25–100/mo | Storage that's rarely read — a small standing cost for optionality |

The throughline: I traded **money and some lock-in for operational simplicity**, and **simplicity for some scale ceilings** that are addressable when the fleet actually gets there. For a Cloud Platform team whose explicit mandate is to not become the bottleneck, those are the right trades for v1 — and each one has a documented escape hatch for when the assumptions stop holding.

---

*Version 1.0 — June 2026*
