# Runnable Slice

## Prerequisites

- Docker and Docker Compose

## Quick Start

```bash
# Start everything (builds images, creates Kafka topics, starts all services)
docker compose up -d

# Wait ~15 seconds for chargers to connect and produce data, then:

# List all chargers with latest state
curl http://localhost:8081/chargers

# Get one charger's state
curl http://localhost:8081/chargers/SPI-00001/state

# Check WSGW health and connection count
curl http://localhost:8080/healthz

# Prometheus-style metrics
curl http://localhost:8080/metrics

# Tear down
docker compose down -v
```

## What's Running

| Container | Role | Port |
|-----------|------|------|
| `kafka` | Apache Kafka (KRaft, single broker) | 9092 |
| `kafka-init` | Creates 5 OCPP topics, then exits | — |
| `redis` | Latest state store + dedup cache | 6379 |
| `postgres` | Event history (append-only) | 5432 |
| `wsgw` | WebSocket gateway (OCPP 1.6-J) | 8080 |
| `state-processor` | Kafka consumer, writes to Redis + PG | — |
| `api` | REST API for latest state + Swagger UI + Fleet Portal | 8081 |
| `cp-simulator` | 50 virtual chargers, 2 connectors each | — |
| `prometheus` | Metrics collection (scrapes all services) | 9090 |
| `grafana` | Dashboards and visualization | 3000 |

## Simulated Charger Behavior

The `cp-simulator` creates 50 chargers (`SPI-00001` through `SPI-00050`), each with 2 connectors. Each charger runs a realistic OCPP session lifecycle:

```
BootNotification -> StatusNotification (Available)
  -> StatusNotification (Preparing)
    -> StartTransaction
      -> StatusNotification (Charging)
        -> MeterValues (energy, power, SoC) every 10s
      -> StatusNotification (Finishing)
    -> StopTransaction
  -> StatusNotification (Available)
-> idle 5-15s -> repeat on random connector
```

Heartbeats are sent every 30 seconds throughout.

## Scaling the Simulator

```bash
# Change NUM_CHARGERS in docker-compose.yml and restart
docker compose up -d
```

## API Examples

**List all chargers:**
```bash
$ curl -s http://localhost:8081/chargers | jq '.count'
50
```

**Single charger state:**
```bash
$ curl -s http://localhost:8081/chargers/SPI-00003/state | jq
{
  "charger_id": "SPI-00003",
  "vendor": "Spirii",
  "model": "S3-50kW",
  "serial": "SPI-00003",
  "firmware": "3.2.1",
  "online": true,
  "last_seen": "2026-06-16T06:57:03Z",
  "last_boot": "2026-06-16T06:49:57Z",
  "connectors": [
    {
      "connector_id": 1,
      "status": "Charging",
      "error_code": "NoError",
      "status_timestamp": "2026-06-16T06:57:03Z",
      "active_transaction": true,
      "energy_wh": "30496",
      "power_w": "25773",
      "soc_percent": "24",
      "meter_timestamp": "2026-06-16T06:57:03Z"
    },
    {
      "connector_id": 2,
      "status": "Finishing",
      "error_code": "NoError",
      "status_timestamp": "2026-06-16T06:55:20Z",
      "active_transaction": true,
      "energy_wh": "43471",
      "power_w": "47941",
      "soc_percent": "68",
      "meter_timestamp": "2026-06-16T06:55:10Z"
    }
  ]
}
```

## Swagger UI

The REST API includes an OpenAPI 3.0 specification and Swagger UI:

- **Swagger UI:** [http://localhost:8081/swagger/](http://localhost:8081/swagger/)
- **OpenAPI spec (JSON):** [http://localhost:8081/swagger/openapi.json](http://localhost:8081/swagger/openapi.json)

The spec documents all endpoints, request/response schemas (including `ChargerState` and `ConnectorState`), OCPP enum values for connector status, and example payloads.

## Fleet Monitoring Portal

A real-time web dashboard for monitoring the entire charger fleet:

- **Fleet Portal:** [http://localhost:8081/dashboard/](http://localhost:8081/dashboard/)

See [docs/fleet-portal.md](fleet-portal.md) for full documentation.

## Observability

**Grafana** is pre-provisioned with a dashboard at [http://localhost:3000](http://localhost:3000) (login: `admin` / `spirii`).

Navigate to **Dashboards > OCPP Charger Telemetry Platform** to see:

| Section | Panels |
|---------|--------|
| **Fleet Overview** | Active WS connections, total connections, events processed, dedup hits, Kafka publish errors, OCPP parse errors |
| **WebSocket Gateway** | OCPP messages/sec by action, Kafka publish latency (p50/p95/p99), connections over time, publish rate by topic |
| **State Processor** | Events processed/sec by type, end-to-end event lag (p50/p95/p99), PostgreSQL write latency, Redis write latency, write errors |
| **REST API** | Requests/sec by endpoint and status code, API latency (p50/p95/p99) |

**Prometheus** is available at [http://localhost:9090](http://localhost:9090) for ad-hoc queries.

**Metrics exposed by each service:**

| Service | Endpoint | Key Metrics |
|---------|----------|-------------|
| WSGW | `:8080/metrics` | `wsgw_active_connections`, `wsgw_ocpp_messages_total{action}`, `wsgw_kafka_publish_duration_seconds`, `wsgw_kafka_publish_total{topic,status}` |
| State Processor | `:8082/metrics` | `processor_events_total{event_type,status}`, `processor_dedup_hits_total`, `processor_pg_write_duration_seconds`, `processor_redis_write_duration_seconds`, `processor_event_lag_seconds` |
| API | `:8081/metrics` | `api_requests_total{endpoint,status}`, `api_request_duration_seconds{endpoint}`, `api_chargers_returned` |

## Makefile Shortcuts

```bash
make up       # start everything
make down     # tear down + remove volumes
make logs     # follow all logs
make logs-wsgw    # follow WSGW logs only
make logs-sim     # follow simulator logs
make logs-proc    # follow state-processor logs
make status   # show health, metrics, and charger state
make test     # start, wait, verify all endpoints respond
make clean    # tear down + prune images
```
