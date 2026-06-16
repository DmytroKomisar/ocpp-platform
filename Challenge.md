# Challenge — Cloud Engineer — Spirii

**Role:** Cloud Platform Engineer

**AI:** allowed, but you must defend every decision as your own. We value trade-offs and reasoning over completeness — scope your effort and tell us what you'd skip.

## Context

Spirii ingests OCPP telemetry from a large and growing fleet of chargers — charging status, power, connector state, errors, meter values. A Cloud Platform Engineer here does two jobs at once: build scalable, secure infrastructure and turn it into a platform other teams build on safely — paved roads, self-service, guardrails — without the Cloud team becoming the bottleneck. This challenge tests both.

## The Problem

Design and build a capability that ingests charger telemetry and exposes the latest known state of any charger, and show how other teams would consume and extend it themselves.

**Requirements:** accept telemetry events, store history, expose latest state, handle duplicate & out-of-order events, support growth in traffic and engineering teams. Any cloud, or cloud-agnostic — if you pick one, call out portability vs. lock-in.

## Deliverables

Keep it short. We'd rather read five tight sections than a long essay.

### 1. Architecture

A diagram and a few paragraphs on how events come in, how they're processed, where history lives, and where latest-state lives. Tell us your data model, and how you keep it correct when events arrive twice or out of order.

### 2. Platform and Developer Experience

This is the part we care about most, so spend your time here. Show us it's a platform, not just a service:

- A second team needs to add a new telemetry event type, or read latest-state for the first time. They shouldn't have to wait on the Cloud team to do it. Walk us through how they'd do it themselves, what's self-serve, what still needs a review, and why.
- Where's the line between what the platform owns and what product teams own, and how do you actually hold that line?
- As more teams come to depend on this over time, how do you keep them safe by default, without the Cloud team reviewing every change by hand?
- How would you get teams to actually use it, and how would you know whether it's any good to build on?

### 3. A Runnable Slice

Something small that actually runs. One ingest path plus a couple of curl calls to write an event and read latest-state is plenty. It doesn't need to be production-ready. Tell us how to run it locally.

### 4. Operations

How you'd deploy it, manage the infrastructure, handle secrets, roll back when something breaks, and what you'd alert on. Tell us what a safe deploy looks like here.

### 5. Trade-offs

What did you assume? What did you leave out on purpose? What breaks first when traffic grows 10x? What would you build next?
