# OCPP Telemetry Platform — Operations

> **Companion to:** Architecture Overview + Platform & Developer Experience
> **Focus:** How it deploys, how infrastructure and secrets are managed, how it rolls back, and what it alerts on — with the stateful WebSocket gateway treated as the special case it is.

---

## Table of Contents

1. [Two deployment planes](#1-two-deployment-planes)
2. [Safe deploy — stateless services](#2-safe-deploy--stateless-services)
3. [Safe deploy — WSGW (the hard part)](#3-safe-deploy--wsgw-the-hard-part)
4. [Infrastructure, GitOps & secrets](#4-infrastructure-gitops--secrets)
5. [Rollback](#5-rollback)
6. [Alerting — what and why](#6-alerting--what-and-why)

---

## 1. Two deployment planes

The platform has two operationally distinct kinds of workload, and conflating them is the most common way to cause an outage here.

| | Stateless plane | Stateful plane (WSGW) |
|---|---|---|
| Examples | Stream Processor, Transaction Processor, REST API, consumers | WebSocket Gateway |
| State | None (state in Kafka/DDB/RDS) | Tens of thousands of live, long-lived connections per pod |
| Deploy risk | Low — restart freely | High — a restart disconnects thousands of chargers |
| Strategy | Canary / blue-green, fast | Drained rolling update, off-peak, rate-bounded |
| Rollback | Instant | Itself a reconnect cycle — not free |

Everything in the stateless plane deploys with standard progressive delivery. The WSGW needs its own playbook, because **a deploy is, by definition, a controlled mass-reconnect event.**

---

## 2. Safe deploy — stateless services

Standard progressive delivery with automated gates. Nothing exotic, because these workloads hold no connection state.

```
1. CI builds image, runs tests, pushes to ECR (immutable tag = git SHA).
2. GitOps (Argo CD / Argo Rollouts) detects the new tag.
3. Canary: route ~5% of traffic / partitions to the new version.
4. Automated analysis gate (2–5 min):
     - error rate, latency p99, consumer lag — compared to baseline
     - if any SLI regresses beyond threshold → auto-rollback
5. Progressive promotion: 5% → 25% → 50% → 100%, gated at each step.
6. Old version stays warm until 100% healthy, enabling instant rollback.
```

For Kafka consumers specifically, "canary" means a small number of new-version pods join the consumer group alongside old-version pods; if their processing lag or error rate regresses, they're removed before full promotion. Feature flags decouple *deploy* from *release* — code ships dark, then is enabled per-tenant.

---

## 3. Safe deploy — WSGW (the hard part)

A WSGW pod holds ~8,000 live WebSocket connections. Replacing it disconnects those chargers, which then reconnect. There is no native WebSocket hand-off in OCPP, so the connections *will* re-establish — the entire job of a safe WSGW deploy is to **bound the reconnect rate so it never becomes a thundering herd**, and to do it when it hurts least.

**First, the reassurance:** a WSGW reconnect does **not** stop physical charging. An in-progress session continues on the charger; only the telemetry link blips and the charger buffers events locally, flushing them on reconnect (see architecture §7). The risk being managed is reconnect load and a brief telemetry gap — not interrupted charging.

### The technique

```
Rolling update, tuned to cycle only a small slice at a time:

  maxUnavailable: 0        # never drop below full capacity
  maxSurge:       1        # bring up one new pod before retiring one old
  terminationGracePeriodSeconds: 300

PreStop sequence on the retiring pod:
  1. Fail readiness probe → NLB deregisters the pod (no NEW connections).
  2. Sleep (drain window): existing connections keep working; the pod keeps
     ingesting from its chargers while the replacement warms up.
  3. On grace-period end: send a clean WebSocket close frame to each charger
     (not an abrupt TCP reset) so the charger reconnects promptly rather than
     waiting for a keepalive timeout.
  4. Chargers reconnect via NLB → land on other pods, spread by the
     firmware's exponential-backoff + jitter.
```

Because `maxSurge: 1`, only **one pod's worth of chargers (~8k)** are ever in reconnect at once — and jitter spreads even those over tens of seconds. The rollout pauses between pods until connection counts re-stabilise, so reconnect bursts never stack.

### Canary for stateful, too

WSGW can still be canaried. Run N−1 old pods plus 1 new pod behind the same NLB; new connections distribute across all pods, so the new version starts taking real traffic immediately. Watch the canary pod's connection count, OCPP message error rate, and ingestion lag for a few minutes. If it misbehaves, only that one pod's chargers are affected — and they reconnect onto healthy old pods. Only then promote the rest.

### Timing

WSGW deploys run in the **regional low-traffic window** (overnight), when fewer sessions are active and a telemetry blip costs least. Routine deploys are scheduled; emergency security patches override the window but still use the rate-bounded rolling technique.

---

## 4. Infrastructure, GitOps & secrets

### Infrastructure as code

```
Everything is Terraform. No console changes — ever.

  - Modular: reusable modules are the "paved roads" product teams instantiate
    (a Kafka topic, a consumer service, a scoped IAM role).
  - PR-based: terraform plan runs in CI on every PR; the plan is the review.
  - Policy as code: OPA/Conftest gates the plan (no public buckets, retention
    within policy, PII tagged, least-privilege IAM).
  - Drift detection: scheduled plan flags any out-of-band change; the repo is
    the single source of truth and drift is reconciled, not tolerated.
```

### GitOps

Application and cluster state are reconciled from git by Argo CD. A deploy is a merged PR; the cluster converges to match. This gives a full audit trail (who changed what, when) and makes rollback a `git revert`.

### Secrets

No static credentials live in code, images, or pods.

```
Workload → AWS access:   IRSA (IAM Roles for Service Accounts).
                         Pods assume scoped IAM roles via OIDC — no keys.
App secrets (DB, Redis): AWS Secrets Manager, synced into the cluster by the
                         External Secrets Operator; rotated on a schedule.
Charger Basic Auth creds: Secrets Manager, per-charger (Security Profile 2).
TLS server keys:         ACM (managed, auto-rotated).
CA private keys:         ACM PCA, HSM-backed, never exported.
```

The guarantee: a leaked container image or git repo exposes **no usable secret**, because secrets are referenced and injected at runtime, never baked in.

---

## 5. Rollback

Rollback strategy differs by what's being rolled back — and two things on this platform are *not* trivially reversible.

```
Stateless services:   instant. The previous version is warm (blue-green) or
                      one git revert away; Argo reconciles in seconds.

WSGW:                 a rollback is another deploy → another reconnect cycle.
                      It is correct but not free. This raises the bar for
                      WSGW canaries: catch problems on one pod BEFORE full
                      rollout, so a full rollback is rarely needed.

Database migrations:  expand-contract pattern. New code works against both old
                      and new schema; the destructive "contract" step ships
                      only after the new code is proven. Rollback = deploy old
                      code, schema still compatible. Never a down-migration
                      under load.

Schema registry:      additive-only by policy, so there is nothing to roll back —
                      old consumers are unaffected by a new field. A bad new
                      event type is simply deprecated and stops being produced;
                      it cannot have broken anyone (that's what the compatibility
                      gate guarantees).
```

The principle: make forward-compatibility the default so that "rollback" almost always means "redeploy previous code," never "undo a data change."

---

## 6. Alerting — what and why

Alert on **symptoms users and chargers feel**, plus a few **leading indicators** that predict pain. Page on what needs a human now; everything else is a ticket or a dashboard. The goal is a quiet pager, not a complete one.

### Page-worthy (SLO burn + critical leading indicators)

| Alert | Why it pages |
|---|---|
| Ingestion availability SLO burn (fast burn-rate) | Chargers can't deliver telemetry — core function down |
| Aggregate connection count drops sharply | Mass disconnect — ingestion or NLB problem |
| End-to-end event lag p99 > threshold | Data is stale; latest-state and billing fall behind |
| Latest-state API error rate / latency SLO burn | Consumers (driver app, ops) are being served bad data |
| Certificate expiry < 7 days (any cohort) | Affected chargers will go fully offline — must act before |
| Kafka consumer lag growing unbounded | A consumer is stuck; backlog will breach retention → data loss |
| DLQ depth rising | Commands or events failing repeatedly — silent failure |

### Ticket / dashboard (investigate, not 3am)

```
- Per-AZ connection imbalance (rebalances on its own, but worth watching)
- Hanging-transaction reconciliation rate climbing (revenue/billing signal)
- Reconnect-storm rate after a deploy (validates the deploy technique)
- Per-tenant ingestion anomalies (one tenant's chargers behaving oddly)
- Cost anomalies (sudden Timestream/Kafka spend increase)
```

### Why SLO-based, not threshold-spam

Alerting on every CPU spike or pod restart trains the team to ignore the pager. Instead, alerts are tied to **error budgets**: a fast budget burn pages immediately; a slow burn opens a ticket. This keeps the signal-to-noise high enough that a page genuinely means "drop what you're doing." Certificate expiry is the notable exception to pure symptom-based alerting — it's a leading indicator where waiting for the symptom (chargers offline) is already too late.

---

*Version 1.0 — June 2026*
