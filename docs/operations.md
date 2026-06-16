# Operations

## Deployment (Production)

**Infrastructure as Code:** Terraform modules for each component:
- EKS cluster with dedicated node groups (WSGW gets its own — stateful, high file descriptor usage)
- Amazon MSK (Kafka) — 3 brokers across 3 AZs, RF=3, min.insync.replicas=2
- ElastiCache Redis — cluster mode, 3 shards with replicas
- RDS PostgreSQL — Multi-AZ, read replicas for analytics
- NLB (TCP passthrough) for WebSocket connections

**CI/CD Pipeline:**

```
PR opened
  -> lint + unit tests
  -> docker build
  -> integration test (docker-compose, full pipeline)
  -> security scan (Trivy)
  -> Terraform plan (for infra changes)

Merge to main
  -> build + push to ECR
  -> Terraform apply (if infra changed)
  -> Rolling deploy to EKS (maxUnavailable: 0, maxSurge: 2)
  -> Smoke test (health checks + synthetic charger event)
  -> Canary (5% traffic for 10 min, auto-rollback on error rate)
```

## Secrets Management

- **Kafka credentials:** AWS MSK IAM authentication — no passwords, uses IAM roles attached to K8s service accounts via IRSA
- **PostgreSQL:** Credentials in AWS Secrets Manager, rotated automatically, injected via External Secrets Operator
- **Redis:** Auth token in Secrets Manager, TLS in-transit
- **API keys:** Stored in Secrets Manager, validated at the API gateway layer

## Rollback

| Scenario | Rollback Method |
|----------|----------------|
| Bad application code | `kubectl rollout undo deployment/wsgw` — instant rollback to previous ReplicaSet |
| Bad Kafka schema | Deploy previous image version. Kafka consumers can replay from last committed offset. |
| Bad DB migration | Forward migration to fix. Migrations are additive-only (no DROP, no column removal) so old code works against new schema. |
| Infra misconfiguration | `terraform apply` with previous state. Terraform state is versioned in S3. |

## Alerting

| Alert | Condition | Severity |
|-------|-----------|----------|
| WSGW connections drop >20% in 5 min | Charger fleet is disconnecting | Critical |
| Kafka consumer lag > 10,000 messages | State processor falling behind | Warning |
| Kafka consumer lag > 100,000 messages | State processor stuck or crashed | Critical |
| API p99 latency > 500ms | Latest state reads are slow | Warning |
| PostgreSQL disk usage > 80% | History table growing faster than retention | Warning |
| Redis memory > 80% | State store approaching capacity | Warning |
| State processor error rate > 1% | Dedup or write failures | Critical |
| Zero messages on any topic for 10 min | Pipeline stalled or chargers disconnected | Critical |

## What a Safe Deploy Looks Like

1. **PR reviewed and merged.** Integration tests passed.
2. **Image built and pushed** to ECR with commit SHA tag (never `:latest` in prod).
3. **Rolling update** — new pods start alongside old pods. Old pods drain connections gracefully (300s `terminationGracePeriodSeconds`). Chargers reconnect to new pods via NLB.
4. **Canary** — 5% of traffic routed to new version. Monitor error rate and latency for 10 minutes.
5. **Full rollout** — if canary is clean, roll to 100%.
6. **Post-deploy** — verify consumer lag is stable, connections are balanced, API latency is nominal.

For WSGW specifically: chargers handle disconnection gracefully (they reconnect with backoff). The NLB's 300s deregistration delay and the pod's `preStop` sleep ensure connections drain before the pod is killed. No thundering herd.
