# OCPP Protocol Reference for Spirii Challenge

## 1. What is OCPP?

**Open Charge Point Protocol (OCPP)** is the global open standard for communication between EV charging stations (Charge Points) and a Central System (CSMS — Charge Station Management System). It is maintained by the Open Charge Alliance (OCA).

### Architecture

```
┌──────────────┐         WebSocket / SOAP         ┌──────────────────┐
│ Charge Point │  ──────────────────────────────►  │  Central System  │
│   (station)  │  ◄──────────────────────────────  │     (CSMS)       │
└──────────────┘      bidirectional messages       └──────────────────┘
```

- **Charge Point (CP)** — the physical charger. Initiates the WebSocket connection outward.
- **Central System (CSMS)** — the backend/cloud. Accepts connections, receives telemetry, sends commands.
- The CP is the **WebSocket client**; the CSMS is the **WebSocket server**.
- Each CP maintains a persistent WebSocket connection to the CSMS.

### Versions

| Version | Transport | Status | Notes |
|---------|-----------|--------|-------|
| 1.5 | SOAP/HTTP | Legacy | Rarely used today |
| **1.6** | SOAP or **JSON over WebSocket** | **Most widely deployed** | Industry standard |
| 2.0.1 | JSON over WebSocket | Growing adoption | Major rewrite, not backward-compatible with 1.6 |
| 2.1 | JSON over WebSocket | Released 2025 | Adds V2X, ISO 15118-20, battery swap |

**For this challenge, OCPP 1.6-J (JSON) is the pragmatic baseline** — it covers the vast majority of deployed chargers. The design should be extensible to 2.0.1+.

---

## 2. OCPP 1.6 Message Format (JSON variant)

OCPP-J uses a simple JSON-over-WebSocket wire format. Every message is a JSON array:

### Message Types

| TypeId | Name | Direction | Format |
|--------|------|-----------|--------|
| 2 | **CALL** | Initiator → Responder | `[2, "<messageId>", "<action>", {payload}]` |
| 3 | **CALLRESULT** | Responder → Initiator | `[3, "<messageId>", {payload}]` |
| 4 | **CALLERROR** | Responder → Initiator | `[4, "<messageId>", "<errorCode>", "<errorDesc>", {details}]` |

- `messageId` — a unique ID (UUID) for request/response correlation.
- `action` — the RPC name, e.g. `StatusNotification`, `MeterValues`.

### Example: StatusNotification

**CALL (CP → CSMS):**
```json
[2, "19223201", "StatusNotification", {
  "connectorId": 1,
  "errorCode": "NoError",
  "status": "Charging",
  "timestamp": "2026-06-15T10:30:00.000Z",
  "info": "",
  "vendorId": "",
  "vendorErrorCode": ""
}]
```

**CALLRESULT (CSMS → CP):**
```json
[3, "19223201", {}]
```

---

## 3. Telemetry-Relevant Messages (CP → CSMS)

These are the messages the challenge cares about — the ones that produce telemetry events for ingestion.

### 3.1 BootNotification

Sent when a CP boots or reconnects. Contains charger identity.

```json
{
  "chargePointVendor": "Spirii",
  "chargePointModel": "S3-50kW",
  "chargePointSerialNumber": "SPI-001234",
  "firmwareVersion": "3.2.1",
  "meterType": "ABB",
  "meterSerialNumber": "MTR-98765"
}
```

**Response includes `heartbeatInterval` (seconds) and `status`** (`Accepted` / `Pending` / `Rejected`).

### 3.2 StatusNotification

The **primary state message**. Sent whenever a connector's status changes.

```json
{
  "connectorId": 1,
  "errorCode": "NoError",
  "status": "Charging",
  "timestamp": "2026-06-15T10:30:00.000Z",
  "info": "optional free-text",
  "vendorId": "Spirii",
  "vendorErrorCode": ""
}
```

**`connectorId`:** 0 = the whole charge point; 1, 2, ... = individual connectors/plugs.

### 3.3 MeterValues

Periodic or triggered energy/power readings. The richest telemetry message.

```json
{
  "connectorId": 1,
  "transactionId": 12345,
  "meterValue": [
    {
      "timestamp": "2026-06-15T10:30:00.000Z",
      "sampledValue": [
        {
          "value": "15234",
          "context": "Sample.Periodic",
          "format": "Raw",
          "measurand": "Energy.Active.Import.Register",
          "phase": null,
          "location": "Outlet",
          "unit": "Wh"
        },
        {
          "value": "22000",
          "measurand": "Power.Active.Import",
          "unit": "W"
        },
        {
          "value": "48.2",
          "measurand": "SoC",
          "unit": "Percent"
        }
      ]
    }
  ]
}
```

### 3.4 Heartbeat

Periodic keepalive. Contains no payload (`{}`). The response contains `currentTime`. Useful to know the CP is alive.

### 3.5 StartTransaction / StopTransaction

Mark the beginning/end of a charging session.

**StartTransaction:**
```json
{
  "connectorId": 1,
  "idTag": "RFID-ABC123",
  "meterStart": 12000,
  "timestamp": "2026-06-15T10:00:00.000Z",
  "reservationId": null
}
```

**StopTransaction:**
```json
{
  "transactionId": 12345,
  "idTag": "RFID-ABC123",
  "meterStop": 27234,
  "timestamp": "2026-06-15T11:30:00.000Z",
  "reason": "EVDisconnected",
  "transactionData": [ /* optional MeterValue array */ ]
}
```

### 3.6 DiagnosticsStatusNotification / FirmwareStatusNotification

Operational messages about firmware updates and diagnostics uploads.

```json
{ "status": "Uploaded" }       // DiagnosticsStatusNotification
{ "status": "Installing" }     // FirmwareStatusNotification
```

---

## 4. Enumerations (Key Values)

### ChargePointStatus (connector status)

| Status | Meaning |
|--------|---------|
| `Available` | Connector is free, ready to charge |
| `Preparing` | Connector plugged in, not yet charging |
| `Charging` | Actively delivering energy |
| `SuspendedEVSE` | Charging suspended by charger (e.g. load balancing) |
| `SuspendedEV` | Charging suspended by vehicle (e.g. battery full) |
| `Finishing` | Transaction stopping, connector still occupied |
| `Reserved` | Connector reserved for a specific user |
| `Unavailable` | Connector not available (out of service) |
| `Faulted` | Error state |

### ChargePointErrorCode

| Code | Meaning |
|------|---------|
| `NoError` | Normal operation |
| `ConnectorLockFailure` | Physical lock issue |
| `EVCommunicationError` | Vehicle communication failed |
| `GroundFailure` | Ground fault detected |
| `HighTemperature` | Overheating |
| `InternalError` | Generic internal error |
| `OtherError` | Vendor-specific |
| `OverCurrentFailure` | Current too high |
| `OverVoltage` | Voltage too high |
| `PowerMeterFailure` | Meter malfunction |
| `PowerSwitchFailure` | Relay/contactor failure |
| `ReaderFailure` | RFID reader issue |
| `ResetFailure` | Reset command failed |
| `UnderVoltage` | Voltage too low |
| `WeakSignal` | Communication signal weak |

### Measurand (MeterValues)

| Measurand | Unit | Description |
|-----------|------|-------------|
| `Energy.Active.Import.Register` | Wh | Total energy delivered (cumulative) |
| `Energy.Active.Export.Register` | Wh | Energy returned to grid (V2G) |
| `Power.Active.Import` | W | Current power draw |
| `Power.Active.Export` | W | Power returned to grid |
| `Current.Import` | A | Current flowing to vehicle |
| `Voltage` | V | Voltage at connector |
| `Temperature` | Celsius | Connector/cable temperature |
| `SoC` | Percent | State of Charge (if reported by vehicle) |
| `Frequency` | Hz | Grid frequency |

### SampledValue Context

| Context | Meaning |
|---------|---------|
| `Sample.Periodic` | Regularly scheduled reading |
| `Sample.Clock` | Aligned to clock interval |
| `Transaction.Begin` | At start of transaction |
| `Transaction.End` | At end of transaction |
| `Trigger` | Triggered by CSMS request |
| `Interruption.Begin` | At start of power interruption |
| `Interruption.End` | At end of power interruption |

---

## 5. OCPP 2.0.1 Key Differences

For forward-compatibility awareness:

| Aspect | OCPP 1.6 | OCPP 2.0.1 |
|--------|----------|------------|
| Hierarchy | ChargePoint → Connectors | ChargingStation → EVSE → Connectors |
| Transactions | `StartTransaction` / `StopTransaction` | `TransactionEvent` (single unified message) |
| Status | `StatusNotification` with flat fields | `StatusNotification` per EVSE + Connector |
| Security | Optional TLS | Mandatory security profiles |
| Device model | Flat key-value config | Structured component/variable model |
| IDs | `chargePointSerialNumber` | `serialNumber` under `ChargingStation` |

### OCPP 2.0.1 Hierarchy

```
ChargingStation (the box)
  ├── EVSE 1 (charging position)
  │     ├── Connector 1 (CCS plug)
  │     └── Connector 2 (Type 2 plug)
  └── EVSE 2
        └── Connector 1 (CCS plug)
```

This matters for data model design — a charger can have multiple EVSEs, each with multiple connectors.

---

## 6. Data Model Implications for the Challenge

### Entity Relationships

```
Charger (charge_point_id)
  └── Connector (connector_id: 0..N)
        ├── has current Status (status, error_code, timestamp)
        ├── has current MeterValues (power, energy, soc, ...)
        └── may have active Transaction (transaction_id)
```

### "Latest State" — What Constitutes It?

The challenge asks for "latest known state of any charger." This is a composite view:

```json
{
  "charger_id": "SPI-001234",
  "last_seen": "2026-06-15T10:30:00Z",
  "online": true,
  "vendor": "Spirii",
  "model": "S3-50kW",
  "firmware_version": "3.2.1",
  "connectors": [
    {
      "connector_id": 1,
      "status": "Charging",
      "error_code": "NoError",
      "status_timestamp": "2026-06-15T10:30:00Z",
      "active_transaction_id": 12345,
      "meter_values": {
        "energy_wh": 15234,
        "power_w": 22000,
        "soc_percent": 48.2,
        "voltage_v": 400,
        "current_a": 55,
        "timestamp": "2026-06-15T10:30:00Z"
      }
    },
    {
      "connector_id": 2,
      "status": "Available",
      "error_code": "NoError",
      "status_timestamp": "2026-06-15T10:25:00Z",
      "active_transaction_id": null,
      "meter_values": null
    }
  ]
}
```

### Handling Duplicates and Out-of-Order Events

1. **Duplicates:** Each OCPP CALL has a `messageId` (UUID). Use this as an **idempotency key**. If the same `messageId` is received again, skip processing.

2. **Out-of-order events:** Events carry a `timestamp` field from the charger. When updating latest state:
   - Compare incoming event's `timestamp` against the stored `last_updated_at` for that field.
   - Only apply the update if `event.timestamp > stored.timestamp` (last-write-wins by event time, not arrival time).
   - Always write to history regardless (append-only log records everything).

3. **Clock skew:** Charger clocks may drift. The CSMS sends `currentTime` in Heartbeat and BootNotification responses to help CPs sync. Design should tolerate small skew (~seconds) but flag large skew (>5 min) as operational alerts.

### Event Envelope for Storage

Every telemetry event, regardless of OCPP message type, can be normalized into:

```json
{
  "event_id": "uuid-v4",
  "message_id": "original-ocpp-message-id",
  "charger_id": "SPI-001234",
  "connector_id": 1,
  "event_type": "StatusNotification",
  "timestamp": "2026-06-15T10:30:00Z",
  "received_at": "2026-06-15T10:30:01.234Z",
  "payload": { /* original OCPP payload */ }
}
```

This normalized envelope is what gets written to the history store and used to update latest state.

---

## 7. Traffic Characteristics

### Typical Message Frequency Per Charger

| Message | Frequency | Notes |
|---------|-----------|-------|
| Heartbeat | Every 30–300s | Configurable via BootNotification response |
| StatusNotification | On state change | ~5-15 per session |
| MeterValues | Every 10–60s during charging | Configurable, biggest volume driver |
| Start/StopTransaction | Once per session start/end | Low frequency |
| BootNotification | On boot / reconnect | Very low frequency |

### Rough Scale Estimates

| Fleet Size | Concurrent Charging | Messages/sec (peak) |
|------------|--------------------|--------------------|
| 1,000 chargers | ~200 | ~50 msg/s |
| 10,000 chargers | ~2,000 | ~500 msg/s |
| 100,000 chargers | ~20,000 | ~5,000 msg/s |

Main volume driver: `MeterValues` during active charging sessions. A 10x traffic growth challenge likely pushes from hundreds to thousands of messages/sec — well within the range of Kafka/Kinesis + a modest database.

---

## 8. Connection Management

### WebSocket Lifecycle

1. CP opens WebSocket to `wss://csms.example.com/ocpp/<chargePointId>`
2. CP sends `BootNotification`
3. CSMS responds with `Accepted` + `heartbeatInterval`
4. CP sends `Heartbeat` at the configured interval
5. CP sends telemetry (StatusNotification, MeterValues, etc.) as events occur
6. CSMS can send commands (RemoteStartTransaction, Reset, etc.) at any time
7. If connection drops, CP reconnects and re-sends `BootNotification`

### Implications for Ingestion Architecture

- The CSMS must handle **persistent WebSocket connections** (one per charger).
- At 10k chargers, that's 10k concurrent WebSocket connections — a dedicated WebSocket gateway is appropriate.
- The gateway's job is to terminate OCPP, produce normalized events to a message queue, and forward commands back.
- **Separation of concerns:** The WebSocket/OCPP layer should be decoupled from the telemetry processing layer.

```
Chargers (WebSocket) → OCPP Gateway → Message Queue → Event Processor → Stores
                                            │
                                     (history + latest state)
```

---

## 9. Security Considerations

| Aspect | OCPP 1.6 | OCPP 2.0.1 |
|--------|----------|------------|
| Transport | TLS optional but recommended | TLS mandatory |
| Auth | Basic Auth or client certificates | Security Profiles 1-3 |
| CP identity | URL path (`/ocpp/<cpId>`) | Same + device certificates |

For the challenge:
- TLS termination at load balancer / API gateway
- CP authentication via connection path or token
- API authentication for consumers via API keys or OAuth2

---

## 10. Version Assumption: Mixed OCPP 1.6 / 2.0.1

Based on analysis of Spirii's public developer API (`developer.spirii.com`):

- The API uses **"chargeboxes"** — OCPP 1.6 terminology (2.0.1 uses "ChargingStation")
- The API has a separate **EVSE** entity — the OCPP 2.0.1 hierarchy (1.6 only has ChargePoint → Connectors)
- Being **hardware-agnostic** with a growing fleet means supporting whatever protocol chargers speak

**Conclusion:** Spirii almost certainly runs a **mixed 1.6 + 2.0.1 fleet**, which is the industry norm for any CPO operating at scale.

**Design approach:**
- Build the runnable slice around **OCPP 1.6-J** messages (simpler, most deployed)
- Design the data model with the **2.0.1 hierarchy** (Station → EVSE → Connector) since it's a superset
- The WSGW gateway normalizes both protocol versions into a **unified event envelope** — downstream services are version-agnostic
- 1.6 connectors map to EVSEs with a single connector in the unified model

---

## 11. What This Means for the Challenge Deliverables

### Architecture decisions driven by OCPP:
1. **WebSocket gateway** — terminates OCPP, produces normalized events
2. **Event envelope normalization** — uniform schema regardless of OCPP message type
3. **Idempotency on `messageId`** — handles duplicate events
4. **Timestamp-based ordering** — handles out-of-order events
5. **Composite latest-state** — per-charger, per-connector view aggregated from multiple event types
6. **Schema extensibility** — new event types (new measurands, OCPP 2.0.1 messages) should be addable without changing the core pipeline

### Platform decisions driven by multi-team use:
1. **Event type registry** — teams can register new event types (new measurands, new OCPP messages) without modifying core pipeline
2. **Schema validation** — validate incoming events against registered schemas
3. **Consumer API** — REST API for latest-state, possibly event streaming for history consumers
4. **Connector-level granularity** — API should support querying by charger, by connector, or across fleet

### Runnable slice (implemented):
- `cmd/cp-simulator` — simulates N chargers over real OCPP 1.6-J WebSocket
- `cmd/wsgw` — WebSocket gateway, parses OCPP, produces to Kafka
- `cmd/state-processor` — Kafka consumer, dedup, writes history (PostgreSQL) + latest state (Redis)
- `cmd/api` — REST API: `GET /chargers`, `GET /chargers/{id}/state`
- All six OCPP message types: BootNotification, StatusNotification, MeterValues, Heartbeat, StartTransaction, StopTransaction
- `docker compose up -d` → full pipeline running locally
