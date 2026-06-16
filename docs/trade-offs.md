# Trade-offs

## Assumptions

- **OCPP 1.6-J is the baseline.** Most deployed chargers speak 1.6. The gateway normalizes both 1.6 and 2.0.1 into a common envelope, so the rest of the pipeline is version-agnostic.
- **Mixed protocol fleet.** Spirii's public API uses both 1.6 terminology ("chargeboxes") and 2.0.1 concepts (EVSE entity), confirming a mixed fleet.
- **AWS as the primary cloud**, with portable core components (Kafka, Redis, PostgreSQL are all open-source). The lock-in points are MSK, ElastiCache, EKS, and NLB — replaceable with self-managed equivalents or GCP/Azure analogs at moderate effort.
- **Eventual consistency is acceptable** for latest state. A few seconds of delay between a charger event and the API reflecting it is fine for all known use cases.

## What I Left Out on Purpose

| Omission | Reason |
|----------|--------|
| OCPP command routing (backend -> charger) | The challenge focuses on telemetry ingestion, not bidirectional control. The architecture doc covers the Redis Pub/Sub pattern for this. |
| Authentication on the REST API | Not relevant for the runnable slice. In production: API keys or OAuth2 at the gateway. |
| TLS on WebSocket connections | The simulator runs locally. In production: TLS termination at the NLB. |
| Schema registry (Avro/Protobuf) | JSON is simpler for the demo. In production: a schema registry prevents breaking changes and enables automated compatibility checks. |
| Multi-region | Single-region is sufficient for the EU market. Cross-region adds complexity that isn't justified by the current fleet size. |
| Charger offline detection | Would need a TTL-based check comparing `last_seen` against a threshold. Straightforward to add but not core to the challenge. |

## What Breaks First at 10x Traffic

Currently: 50 chargers, ~500 msg/s (including heartbeats and meter values).

At 10x (500 chargers, ~5,000 msg/s) — **nothing breaks.** All components handle this trivially.

At real 10x production scale (50k -> 500k chargers, ~25,000 msg/s):

1. **WSGW memory.** Each WebSocket connection holds ~20KB of state. At 500k connections across the fleet, that's ~10GB — need more pods (from 8 to ~60) and dedicated node groups.
2. **Kafka partitions.** The `ocpp.meter_values` topic would need 72+ partitions (up from 36) to maintain consumer parallelism at the higher throughput.
3. **Redis memory.** Latest state for 500k chargers with 2 connectors each: ~500k x 2 x ~500 bytes = 500MB — Redis handles this easily. Not the bottleneck.
4. **PostgreSQL write throughput.** At 25k inserts/sec, a single writer would need batching or partitioned tables. TimescaleDB or moving history to S3 + Parquet would be the next step.

## What I'd Build Next

1. **Schema registry** — enforce backward-compatible schema evolution for Kafka messages
2. **Charger offline detection** — compare `last_seen` against configurable threshold, emit alerts
3. **Command routing** — Redis Pub/Sub pattern for backend -> charger commands (RemoteStart, Reset)
4. **Terraform modules** — production IaC for all infrastructure components
5. **Event replay** — ability to rebuild Redis state from PostgreSQL history (disaster recovery)
6. **Per-charger Grafana drill-down** — variable-based dashboard to inspect individual charger history
7. **Self-service Terraform module** — product teams can provision their own Kafka consumer group + read replica access in one PR
