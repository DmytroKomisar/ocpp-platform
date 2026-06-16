# Platform and Developer Experience

## Adding a New Event Type (Self-Serve)

A second team wants to add a new telemetry event type — say `DiagnosticsStatusNotification`. Here's how they do it without waiting on the Cloud team:

**What's self-serve:**

1. **Define the payload struct** in `internal/ocpp/payloads.go` — add the Go struct for the new message type
2. **Add the Kafka topic** in `internal/kafka/topics.go` — one line
3. **Add the WSGW handler** in `cmd/wsgw/main.go` — add a case to `processCall()` mapping the action to its response and topic
4. **Add the state update** in `cmd/state-processor/main.go` — add a case to `updateLatestState()` defining which Redis keys to write
5. **Open a PR** — the existing CI pipeline validates compilation, linting, and integration tests

**What still needs review, and why:**

- The PR itself — to catch schema mismatches, verify idempotency is preserved, and ensure the new topic has proper retention. This is a 15-minute code review, not a multi-day platform ticket.
- Kafka topic creation in production — topics are pre-created via IaC (Terraform), so the team adds a topic resource to the shared module. The Cloud team reviews the Terraform plan, not the application code.

**What the team doesn't need to do:**

- Touch the pipeline infrastructure (Kafka, Redis, PostgreSQL)
- Modify the event envelope schema
- Change any deployment configuration
- Coordinate a release with the Cloud team

## Reading Latest State (Self-Serve)

A team wants to read charger state for the first time:

1. **REST API** — `GET /chargers/{id}/state` requires no onboarding. It returns the composite view.
2. **Kafka consumer** — if they need streaming access to raw events, they create a new consumer group on the relevant `ocpp.*` topic. No coordination needed; Kafka handles consumer group isolation natively.
3. **PostgreSQL** — for historical queries and analytics, they get a read replica connection. Schema is documented, queries are their own.

## Ownership Boundaries

```
+--------------------------------------------------------------+
|                    Platform Team Owns                          |
|                                                                |
|  WSGW (WebSocket termination, OCPP parsing)                   |
|  Kafka cluster + topic lifecycle                               |
|  Event envelope schema + dedup logic                           |
|  Redis + PostgreSQL infrastructure                             |
|  API gateway + auth                                            |
|  CI/CD pipeline + deployment tooling                           |
|  Monitoring, alerting, SLOs                                    |
+--------------------------------------------------------------+

+--------------------------------------------------------------+
|                    Product Teams Own                           |
|                                                                |
|  Their Kafka consumer logic                                    |
|  Business rules (billing, alerting, analytics)                 |
|  Their own APIs and services                                   |
|  New event type payloads (added via PR to shared repo)         |
|  Dashboards and queries on top of the data                     |
+--------------------------------------------------------------+
```

**How we hold the line:**

- **Shared library, not shared service.** The `internal/` packages define the contract. Teams extend the event types through code, not tickets.
- **Schema validation at the boundary.** The WSGW validates incoming OCPP messages. If a team's new event type produces malformed data, it fails at the gateway — not downstream.
- **Kafka topic isolation.** Each event type gets its own topic. One team's noisy consumer can't slow down another's.

## Safety at Scale

As more teams depend on the platform:

- **Immutable event history.** PostgreSQL history is append-only. No team can corrupt another team's view of the past.
- **Consumer group isolation.** Kafka consumer groups are independent. A slow consumer doesn't block others or create backpressure.
- **Timestamp-guarded writes.** Even if events arrive out of order from different consumers, the latest state is always correct.
- **CI guardrails.** PRs that touch `internal/` run integration tests that spin up the full pipeline (docker-compose) and verify end-to-end event flow. Breaking changes are caught before merge.
- **Rate limiting and quotas** (production). API consumers get per-team rate limits. Kafka consumers get per-group quotas. The platform team monitors lag and alerts on it.

## Adoption and Feedback

**How to get teams to use it:**

- **Make it easier than the alternative.** One `curl` call vs. building your own charger state tracking. The self-serve path is faster than filing a ticket.
- **Golden path documentation.** This README, example `curl` calls, and a starter consumer template.
- **Internal tech talk** — demo the pipeline, show the architecture, answer questions.

**How to know if it's good:**

- **API latency p50/p95/p99** — is the state API fast enough for product use cases?
- **Kafka consumer lag per team** — are consumers keeping up?
- **Time-to-first-event for new teams** — how long from "I want charger data" to "I have charger data"?
- **PR cycle time** — how long does it take to land a new event type?
- **Support ticket volume** — if it's going down, the platform is working.
