# Changelog

## v0.3.0 — Charging Sessions (CDRs) & Realistic Simulator

### Charge Detail Records (CDRs)

- New `charge_sessions` table in PostgreSQL storing completed charging sessions
- State processor pairs `StartTransaction` and `StopTransaction` events to build CDRs automatically
- Each CDR contains: charger ID, connector, ID tag, start/end time, duration, energy consumed (Wh), meter start/stop, stop reason
- Start data stored temporarily in Redis (`tx:active:{chargerID}`) with 24h TTL safety net
- New API endpoint: `GET /sessions?limit=N&charger_id=X` — returns completed sessions ordered by end time (newest first)

### Sessions Dashboard Page

- New "Sessions" tab in the fleet portal navigation bar
- Live-updating table of completed charging sessions at `/dashboard/sessions`
- Columns: time ended, charger ID (clickable — navigates to charger detail), connector, duration, energy, ID tag, stop reason
- Color-coded stop reason badges (green=EVDisconnected, blue=Local, yellow=Remote)
- New rows animate in with a subtle highlight
- Auto-refreshes every 5 seconds

### Realistic Simulator

- **Mostly Available fleet**: ~25% chance per cycle that a connector starts a session, matching real-world utilization patterns
- **Idle periods**: 30s-3min between session attempts (heartbeats continue during idle)
- **10 charger models**: Spirii S3-50kW, Spirii S5-150kW, ABB Terra 54, ABB Terra 124, Kempower S-Series, Alfen Eve Single/Double, Easee Charge, Zaptec Go — each with correct max power ratings
- **Realistic charging curve**: power tapers as SoC increases (mimics real battery behavior)
- **Edge cases**: 5% chance driver plugs in but doesn't authorize (Preparing -> Available), 3% chance of SuspendedEV during charging
- **Varied stop reasons**: EVDisconnected (most common), Local, Remote
- **Finishing state**: driver takes 5-30s to unplug after session ends

### Infrastructure Changes

- API service now connects to PostgreSQL (added `POSTGRES_DSN` env var and `postgres` dependency in docker-compose)
- API service imports `github.com/lib/pq` for PostgreSQL driver

---

## v0.2.0 — Fleet Monitoring Portal & Observability

### Fleet Monitoring Portal

- Real-time web dashboard embedded in the API binary at `/dashboard/`
- Fleet dashboard with aggregate stats: total chargers, online count, charging now, total power, total energy, faulted
- Charger card grid with color-coded status badges, filtering, sorting, and search
- Charger detail page with per-connector view (status, power, energy, SoC progress bar, error codes)
- SPA routing with browser back/forward support
- Dark theme, zero external dependencies, vanilla HTML/CSS/JS
- Auto-refresh: fleet view every 5s, detail view every 3s

### Swagger UI & OpenAPI

- OpenAPI 3.0 specification embedded in the binary at `/swagger/openapi.json`
- Swagger UI served at `/swagger/`
- Documents all endpoints with schemas for ChargerState and ConnectorState

### Observability

- Prometheus metrics on all three services (WSGW, state-processor, API)
- Pre-provisioned Grafana dashboard with 17 panels across 4 sections (Fleet Overview, WebSocket Gateway, State Processor, REST API)
- Prometheus scrape config for all services

### Scaling

- Demonstrated scaling from 5 to 50 chargers live

---

## v0.1.0 — Initial Platform

### Core Pipeline

- WebSocket Gateway (WSGW): terminates OCPP 1.6-J, parses messages, produces normalized events to Kafka
- State Processor: Kafka consumer with deduplication (messageId via Redis SETNX), timestamp-guarded writes, dual-write to PostgreSQL (history) and Redis (latest state)
- REST API: `GET /chargers` (fleet list), `GET /chargers/{id}/state` (single charger detail)
- Charge Point Simulator: configurable virtual chargers with realistic OCPP session lifecycle

### Infrastructure

- Docker Compose with 9 services: Kafka (KRaft), Redis, PostgreSQL, WSGW, state-processor, API, cp-simulator, Prometheus, Grafana
- Kafka topic pre-creation via init container (5 topics, 6 partitions each)
- Health checks and dependency ordering
- Multi-stage Go Dockerfile shared across all services

### OCPP Messages

- BootNotification, StatusNotification, MeterValues, Heartbeat, StartTransaction, StopTransaction
