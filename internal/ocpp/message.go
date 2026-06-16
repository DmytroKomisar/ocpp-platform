package ocpp

import (
	"encoding/json"
	"fmt"
)

// OCPP-J message type IDs
const (
	MessageTypeCall       = 2
	MessageTypeCallResult = 3
	MessageTypeCallError  = 4
)

// Actions
const (
	ActionBootNotification       = "BootNotification"
	ActionStatusNotification     = "StatusNotification"
	ActionMeterValues            = "MeterValues"
	ActionHeartbeat              = "Heartbeat"
	ActionStartTransaction       = "StartTransaction"
	ActionStopTransaction        = "StopTransaction"
)

// ChargePointStatus
const (
	StatusAvailable    = "Available"
	StatusPreparing    = "Preparing"
	StatusCharging     = "Charging"
	StatusSuspendedEV  = "SuspendedEV"
	StatusSuspendedEVSE = "SuspendedEVSE"
	StatusFinishing    = "Finishing"
	StatusReserved     = "Reserved"
	StatusUnavailable  = "Unavailable"
	StatusFaulted      = "Faulted"
)

// ErrorCodes
const (
	ErrorNoError           = "NoError"
	ErrorConnectorLockFail = "ConnectorLockFailure"
	ErrorInternalError     = "InternalError"
	ErrorOtherError        = "OtherError"
)

// Message represents a parsed OCPP-J message.
type Message struct {
	TypeID    int
	MessageID string
	Action    string            // only for CALL
	Payload   json.RawMessage
}

// ParseMessage parses an OCPP-J wire-format JSON array.
func ParseMessage(data []byte) (*Message, error) {
	var raw []json.RawMessage
	if err := json.Unmarshal(data, &raw); err != nil {
		return nil, fmt.Errorf("invalid OCPP message: %w", err)
	}
	if len(raw) < 3 {
		return nil, fmt.Errorf("OCPP message too short: %d elements", len(raw))
	}

	var typeID int
	if err := json.Unmarshal(raw[0], &typeID); err != nil {
		return nil, fmt.Errorf("invalid type ID: %w", err)
	}

	var messageID string
	if err := json.Unmarshal(raw[1], &messageID); err != nil {
		return nil, fmt.Errorf("invalid message ID: %w", err)
	}

	msg := &Message{
		TypeID:    typeID,
		MessageID: messageID,
	}

	switch typeID {
	case MessageTypeCall:
		if len(raw) < 4 {
			return nil, fmt.Errorf("CALL message requires 4 elements")
		}
		if err := json.Unmarshal(raw[2], &msg.Action); err != nil {
			return nil, fmt.Errorf("invalid action: %w", err)
		}
		msg.Payload = raw[3]
	case MessageTypeCallResult:
		msg.Payload = raw[2]
	case MessageTypeCallError:
		msg.Payload = raw[2]
	default:
		return nil, fmt.Errorf("unknown message type: %d", typeID)
	}

	return msg, nil
}

// MakeCallResult builds an OCPP-J CALLRESULT message.
func MakeCallResult(messageID string, payload interface{}) ([]byte, error) {
	p, err := json.Marshal(payload)
	if err != nil {
		return nil, err
	}
	return json.Marshal([]json.RawMessage{
		json.RawMessage(`3`),
		json.RawMessage(fmt.Sprintf(`%q`, messageID)),
		p,
	})
}

// MakeCall builds an OCPP-J CALL message.
func MakeCall(messageID, action string, payload interface{}) ([]byte, error) {
	p, err := json.Marshal(payload)
	if err != nil {
		return nil, err
	}
	return json.Marshal([]json.RawMessage{
		json.RawMessage(`2`),
		json.RawMessage(fmt.Sprintf(`%q`, messageID)),
		json.RawMessage(fmt.Sprintf(`%q`, action)),
		p,
	})
}
