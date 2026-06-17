# OCPP Telemetry Platform — Platform & Developer Experience

> **Companion to:** OCPP EV Charging Platform — AWS Architecture Overview
> **Focus:** How this is a *platform* other teams build on safely — paved roads, self-service, guardrails — not just a service the Cloud team operates.

---

## Table of Contents

1. [Platform vs Service — the framing](#1-platform-vs-service--the-framing)
2. [The ownership line: envelope vs payload](#2-the-ownership-line-envelope-vs-payload)
3. [Self-serve: adding a new telemetry event type](#3-self-serve-adding-a-new-telemetry-event-type)
4. [Self-serve: reading latest-state for the first time](#4-self-serve-reading-latest-state-for-the-first-time)
5. [Safe by default — guardrails without manual review](#5-safe-by-default--guardrails-without-manual-review)
6. [Adoption — getting teams to actually use it](#6-adoption--getting-teams-to-actually-use-it)
7. [Knowing if it's any good — platform metrics](#7-knowing-if-its-any-good--platform-metrics)

---

## 1. Platform vs Service — the framing

A *service* is something the Cloud team runs and others file tickets against. A *platform* is something other teams extend themselves, within guardrails, without the Cloud team in the loop. The difference is entirely about **who can make a change and how.**

The test Spirii poses: a second team needs to add a new telemetry event type, or read latest-state for the first time. If that requires a ticket to the Cloud team, it's a service. The goal is that it requires a pull request to a repo they own, an automated check, and a merge — with the Cloud team involved only when something genuinely risky is proposed.

The mechanism that makes this possible is a single idea: **a stable contract at the boundary, with everything inside the contract owned by product teams and everything around it owned by the platform.**

---

## 2. The ownership line: envelope vs payload

Every event on the platform is an **envelope** wrapping a **payload**.

```json
{
  // ENVELOPE — owned by the platform, identical for every event type
  "eventType":      "SESSION_METER",
  "schemaVersion":  "2",
  "tenantId":       "cpo-acme",
  "chargePointId":  "CP-007",
  "messageId":      "uuid",
  "timestamp":      "2026-06-15T14:23:00Z",
  "ingestedAt":     "2026-06-15T14:23:00.123Z",
  "sequenceNumber": 1847,

  // PAYLOAD — owned by the product team that defined this event type
  "payload": {
    "energyKwh": 12.3,
    "powerW": 7200
  }
}
```

This single split is what holds the whole platform together:

| | Platform owns (the Cloud team) | Product team owns |
|---|---|---|
| **Envelope** | Every field, format, and meaning | — |
| **Payload** | — | Fields, schema, evolution |
| **Transport** | Kafka cluster, topics, partitioning | Their producers / consumers |
| **Ingestion** | WSGW, OCPP adapters, enrichment | — |
| **Latest-state** | Store, API, SLOs | Which fields they read |
| **Schema registry** | The registry + compatibility rules | Their schemas within those rules |
| **Paved roads** | Terraform modules, SDKs, templates | Instantiating them |
| **Guardrails** | Tenancy, quotas, IAM, observability | Operating within them |

**How the line is actually held:** the envelope is not a convention teams are asked to follow — it is *physically enforced* by the producer SDK. A team cannot emit an event without a valid envelope, because the only sanctioned way to produce is through the platform's client library, which stamps the envelope and rejects malformed payloads against the registered schema. The line holds because crossing it is harder than staying inside it.

---

## 3. Self-serve: adding a new telemetry event type

**Scenario:** the Diagnostics team wants to start emitting a new `BATTERY_HEALTH` event from chargers that support it.

They never talk to the Cloud team. Here is the entire flow:

```
1. Diagnostics team writes a schema file in their own repo:
      schemas/battery_health/v1.avsc   (Avro / JSON Schema / Protobuf)
   defining ONLY the payload fields they own.

2. They open a pull request.

3. CI runs automatically (platform-provided GitHub Action):
      ✓ Lint: naming conventions, required envelope compatibility
      ✓ Compatibility check against schema registry:
          - NEW subject, or backward-compatible change → PASS (auto)
          - BREAKING change to an existing schema       → BLOCK (needs review)
      ✓ Policy check (OPA/Conftest): retention sane, no PII without tag

4. PR merges (auto-approved if all checks green and it's additive).

5. GitOps pipeline acts on merge:
      - registers schema in the registry
      - provisions Kafka topic via the standard Terraform module
        (RF=3, partition key {tenantId}#{chargePointId}, default retention)
      - grants the team's service account produce rights — scoped to
        ONLY this topic
      - auto-generates a typed producer client from the schema

6. Diagnostics team deploys their producer using the generated client.
   First BATTERY_HEALTH event flows to prod.
```

**What's self-serve:** defining the payload, registering an additive schema, getting a topic, getting scoped produce rights, getting a typed client. The common case — a new event type or an additive change to an existing one — is fully automated.

**What still needs a human review, and why:**

| Change | Gate | Why |
|---|---|---|
| New event type (additive) | Automated | Cannot break existing consumers |
| Add optional field | Automated | Backward-compatible by definition |
| Remove/rename a field, change a type | **Platform review** | Breaks existing consumers — needs migration plan |
| Cross-tenant data access | **Platform review** | Tenancy boundary — never automated |
| Retention > policy max, or PII fields | **Platform review** | Compliance / cost implications |

The reviewable set is small and high-signal. The Cloud team reviews *breaking changes and boundary crossings*, not routine additions. That is the entire point: human attention is spent only where automation genuinely can't make the call.

---

## 4. Self-serve: reading latest-state for the first time

**Scenario:** the Mobile team wants to show live charger status in the driver app. They've never used the platform before.

```
1. Discover: they open the internal developer portal (Backstage).
      - catalog of event types + schemas
      - the latest-state API spec (OpenAPI)
      - ownership, SLOs, examples, the runnable quickstart

2. Get access: they request read access to the latest-state API via a
   self-serve Terraform module that grants a scoped, read-only,
   tenant-restricted IAM role. No ticket.

3. Read, two ways:
      GET /chargers/{id}/state            → single charger snapshot
      GET /chargers?status=Faulted        → fleet query (DynamoDB GSI)
   via the platform SDK, which handles auth, tenant scoping, retries,
   and pagination for them.

4. Build. The SDK + portal docs + quickstart get them from zero to a
   working read in an afternoon.
```

The platform exposes **two read surfaces**: a synchronous API for point and fleet queries (backed by DynamoDB + Redis), and a subscribe path (consume the Kafka stream directly) for teams that need the firehose. Teams pick based on their need; both are documented and self-serve.

---

## 5. Safe by default — guardrails without manual review

As more teams depend on the platform, "the Cloud team reviews every change" does not scale. Safety has to be a property of the system, not of human vigilance. Each guardrail below runs without a person in the loop:

```
Schema safety:
  Compatibility rules in the registry, enforced in CI.
  Additive changes pass; breaking changes are blocked automatically.
  → a team literally cannot ship a change that breaks another team's consumer.

Tenant isolation:
  Enforced by infrastructure (DynamoDB PK = {tenantId}#..., PostgreSQL RLS,
  S3 prefix IAM). A forgotten WHERE clause still cannot leak cross-tenant data.
  → covered in the architecture doc (§10 Multi-Tenancy).

Blast-radius limits:
  Per-consumer Kafka quotas and per-tenant ingest rate limits are defaults,
  not opt-ins. One team's runaway consumer or one charger's backfill flood
  cannot starve others.

Least privilege:
  Default-deny IAM. The self-serve Terraform modules grant exactly the scoped
  rights needed (produce to one topic, read one tenant) and nothing more.

Observability by default:
  The paved-road service template wires metrics, traces, structured logs, and
  a starter dashboard automatically. A new consumer is observable on day one
  without the team instrumenting anything.

Policy as code:
  OPA/Conftest policies gate the Terraform modules. Teams can self-serve
  infrastructure, but cannot provision something that violates a guardrail
  (public bucket, oversized retention, untagged PII).
```

The pattern is consistent: **the safe path is the default path, and the unsafe path is either blocked or requires review.** Teams move fast because the guardrails let them, not despite them.

---

## 6. Adoption — getting teams to actually use it

A platform nobody uses is shelfware. Adoption is engineered, not assumed:

```
Make the paved road the easiest road.
  A Backstage "new telemetry consumer" template scaffolds a service with
  auth, SDK, CI, deploy, and observability already wired — running in minutes.
  Rolling your own is strictly more work than using the template. Teams take
  the paved road because it is faster, not because they're told to.

Make it discoverable.
  The developer portal is the single place to find event types, schemas, APIs,
  ownership, and examples. No tribal knowledge, no "ask the Cloud team on Slack."

Make the first hour count.
  A runnable quickstart (one ingest path + curl to write an event and read
  latest-state, runs locally) gets a developer to "it works" before they
  commit. Time-to-first-event is the metric that matters here.

Support, then get out of the way.
  Office hours and a responsive support channel for the early adopters of each
  new capability. As patterns stabilise, they move into docs and templates so
  the support load falls instead of growing.
```

The strategic goal restated from the brief: the Cloud team must not become the bottleneck. Every recurring request is a signal to build a paved road that retires it.

---

## 7. Knowing if it's any good — platform metrics

A platform's quality is measured by how well teams build on it, not by infra uptime alone. Two metric families:

**Is it good to build on? (developer experience)**

| Metric | What it tells us | Target direction |
|---|---|---|
| Time-to-first-event | How long from new team → event in prod | Hours/days, not weeks |
| Self-serve ratio | % of changes shipped with zero Cloud-team involvement | > 90% |
| Teams / services onboarded | Adoption breadth | Growing |
| Paved-road adoption | % of services using the template vs bespoke | > 80% |
| Platform NPS / survey | Do developers find it good to build on? | Positive, trending up |

**Is it reliable? (platform SLOs)**

| SLO | Target |
|---|---|
| Ingestion availability | 99.95% |
| Latest-state read latency (p99) | < 50 ms |
| End-to-end event lag (charger → queryable) | p99 < 5 s |
| Schema-check CI time | < 60 s (fast feedback keeps self-serve pleasant) |

The two families are linked: if self-serve ratio is low or time-to-first-event is high, the platform is leaking work back to the Cloud team — the bottleneck is returning, and that is the earliest warning that something needs a new paved road.

---

*Version 1.0 — June 2026*
