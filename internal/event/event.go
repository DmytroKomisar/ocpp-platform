package event

import (
	"encoding/json"
	"time"
)

// Event is the normalized envelope for all OCPP telemetry events.
type Event struct {
	EventID     string          `json:"event_id"`
	MessageID   string          `json:"message_id"`
	ChargerID   string          `json:"charger_id"`
	ConnectorID int             `json:"connector_id"`
	EventType   string          `json:"event_type"`
	Timestamp   time.Time       `json:"timestamp"`
	ReceivedAt  time.Time       `json:"received_at"`
	Payload     json.RawMessage `json:"payload"`
}
