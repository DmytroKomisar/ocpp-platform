# Architecture

## High-Level Diagram

```
EV Chargers (OCPP 1.6-J / 2.0.1)
       |
       |  WebSocket (persistent, one per charger)
       v
+--------------+     Kafka topics      +-------------------+
|  WebSocket   | ------------------->  |  State Processor   |
|  Gateway     |  ocpp.status_notif    |                   |
|  (WSGW)      |  ocpp.meter_values    |  - Dedup (msgId)  |
|              |  ocpp.transactions    |  - Timestamp guard |
|  - Parse     |  ocpp.boot_notif     |                   |
|  - Respond   |  ocpp.heartbeat      |  Writes to:       |
|  - Normalize |                      |  +-------------+  |
|  - Produce   |  key: chargerID      |  | PostgreSQL  |  |
+--------------+  (per-charger order)  |  | (history)   |  |
                                       |  +-------------+  |
                                       |  +-------------+  |
                                       |  | Redis       |  |
                                       |  | (latest st.)|  |
                                       |  +------+------+  |
                                       +---------+---------+
                                                  |
                                       +----------v----------+
                                       |    REST API          |
                                       |                      |
                                       |  GET /chargers       |
                                       |  GET /chargers/{id}  |
                                       |  GET /sessions       |
                                       +----------------------+
```

## Data Flow

1. **Ingest.** Each charger opens a persistent WebSocket connection to the WSGW at `/ocpp/1.6/{chargePointId}`. The WSGW parses OCPP-J messages (CALL), sends the appropriate CALLRESULT, and normalizes the event into a common envelope.

2. **Queue.** Events are produced to Kafka, partitioned by `chargePointId`. This guarantees per-charger ordering and decouples ingestion from processing.

3. **Process.** The state-processor consumes from all `ocpp.*` topics. For each event it:
   - **Deduplicates** on the OCPP `messageId` (Redis SETNX with 1h TTL)
   - **Writes history** to PostgreSQL (append-only, `ON CONFLICT DO NOTHING`)
   - **Updates latest state** in Redis with a **timestamp guard** — only applies the update if the event timestamp is newer than what's stored

4. **Build CDRs.** On `StopTransaction`, the state-processor pairs it with the corresponding `StartTransaction` (cached in Redis) to create a Charge Detail Record (CDR) — charger, connector, duration, energy consumed, stop reason — and writes it to PostgreSQL.

5. **Serve.** The REST API reads latest state from Redis and completed sessions from PostgreSQL.

## Data Model

**Event envelope** (normalized, used across Kafka and PostgreSQL):

```json
{
  "event_id":     "uuid",
  "message_id":   "ocpp-message-id",
  "charger_id":   "SPI-00001",
  "connector_id": 1,
  "event_type":   "StatusNotification",
  "timestamp":    "2026-06-16T10:30:00Z",
  "received_at":  "2026-06-16T10:30:01Z",
  "payload":      { /* original OCPP payload */ }
}
```

**Latest state** (Redis, composite per charger):

```
charger:{id}            -> { vendor, model, firmware, online, last_seen, last_boot }
charger:{id}:conn:{n}   -> { status, error_code, energy_wh, power_w, soc_percent, ... }
charger:{id}:connectors -> SET of connector IDs
chargers                -> SET of all charger IDs
```

**Charge Detail Records** (PostgreSQL, one row per completed session):

```
charge_sessions:
  session_id, charger_id, connector_id, id_tag,
  start_time, end_time, duration_sec,
  energy_wh, meter_start, meter_stop, stop_reason
```

Built automatically by pairing `StartTransaction` and `StopTransaction` events. Start data is cached in Redis (`tx:active:{chargerID}`) during the session.

## Handling Duplicates and Out-of-Order Events

| Problem | Solution |
|---------|----------|
| **Duplicate events** | `messageId` dedup via Redis `SETNX` (1h TTL). PostgreSQL `ON CONFLICT DO NOTHING` as a safety net. |
| **Out-of-order events** | Timestamp guard on Redis writes: `if event.timestamp > stored.timestamp` then update, else skip. History is always written (append-only). |
| **Per-charger ordering** | Kafka partition key = `chargePointId`. All events for one charger go to the same partition, consumed in order. |
