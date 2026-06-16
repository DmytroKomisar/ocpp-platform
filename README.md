# OCPP Charger Telemetry Platform

A platform that ingests OCPP telemetry from EV chargers, stores event history, and exposes the latest known state of any charger via REST API.

Built for the Spirii Cloud Platform Engineer challenge.

## Quick Start

```bash
docker compose up -d
# Wait ~15s, then:
curl http://localhost:8081/chargers
```

| URL | What |
|-----|------|
| [localhost:8081/chargers](http://localhost:8081/chargers) | REST API — fleet state |
| [localhost:8081/dashboard/](http://localhost:8081/dashboard/) | Fleet Monitoring Portal (live) |
| [localhost:8081/swagger/](http://localhost:8081/swagger/) | Swagger UI (OpenAPI 3.0) |
| [localhost:3000](http://localhost:3000) | Grafana dashboards (`admin` / `spirii`) |
| [localhost:9090](http://localhost:9090) | Prometheus |

## Deliverables

| # | Deliverable | Document |
|---|-------------|----------|
| 1 | **Architecture** — data flow, event model, dedup, ordering | [docs/architecture.md](docs/architecture.md) |
| 2 | **Platform & DX** — self-serve onboarding, ownership boundaries, safety at scale | [docs/platform-dx.md](docs/platform-dx.md) |
| 3 | **Runnable Slice** — full local environment, API examples, observability, fleet portal | [docs/runnable-slice.md](docs/runnable-slice.md) |
| 4 | **Operations** — deploy pipeline, secrets, rollback, alerting | [docs/operations.md](docs/operations.md) |
| 5 | **Trade-offs** — assumptions, omissions, 10x scaling analysis | [docs/trade-offs.md](docs/trade-offs.md) |

## Additional Documentation

| Document | Description |
|----------|-------------|
| [docs/fleet-portal.md](docs/fleet-portal.md) | Fleet Monitoring Portal — features, status colors, tech details |
| [docs/fleet-portal-spec.md](docs/fleet-portal-spec.md) | Fleet Portal technical specification |

## Architecture Reference

Background documents on OCPP protocol and production AWS architecture (not part of the challenge deliverables):

| Document | Description |
|----------|-------------|
| [architecture-documents/OCPP-Reference.md](architecture-documents/OCPP-Reference.md) | OCPP 1.6-J protocol reference — messages, enums, data model |
| [architecture-documents/abbreviation.md](architecture-documents/abbreviation.md) | Glossary of OCPP/EV/DevOps terms |
| [architecture-documents/ocpp-aws-architecture.md](architecture-documents/ocpp-aws-architecture.md) | Production AWS architecture for 50k-150k chargers |

## Project Structure

```
.
├── cmd/
│   ├── api/                 # REST API + Fleet Portal + Swagger UI
│   │   ├── dashboard/       # Fleet monitoring portal (HTML/CSS/JS, embedded)
│   │   └── swagger/         # OpenAPI 3.0 spec (embedded)
│   ├── cp-simulator/        # Virtual charge point simulator
│   ├── state-processor/     # Kafka consumer -> PostgreSQL + Redis
│   └── wsgw/                # WebSocket gateway (OCPP 1.6-J)
├── internal/
│   ├── event/               # Normalized event envelope
│   ├── kafka/               # Producer, consumer, topic definitions
│   └── ocpp/                # OCPP message types, wire format, payloads
├── monitoring/
│   ├── prometheus/          # Prometheus scrape config
│   └── grafana/             # Provisioned datasources + dashboards
├── docs/                    # Challenge deliverable documents
├── architecture-documents/  # OCPP reference, glossary, AWS architecture
├── docker-compose.yml       # Full local environment (10 containers)
├── Dockerfile               # Multi-stage Go build (shared across services)
├── Makefile                 # Build, run, test shortcuts
└── README.md                # This file
```
