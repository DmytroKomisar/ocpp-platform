package ocpp

import "time"

// BootNotificationReq is the CP→CSMS BootNotification payload.
type BootNotificationReq struct {
	ChargePointVendor       string `json:"chargePointVendor"`
	ChargePointModel        string `json:"chargePointModel"`
	ChargePointSerialNumber string `json:"chargePointSerialNumber,omitempty"`
	FirmwareVersion         string `json:"firmwareVersion,omitempty"`
}

// BootNotificationConf is the CSMS→CP response.
type BootNotificationConf struct {
	Status      string    `json:"status"`
	CurrentTime time.Time `json:"currentTime"`
	Interval    int       `json:"interval"` // heartbeat interval in seconds
}

// StatusNotificationReq is the CP→CSMS StatusNotification payload.
type StatusNotificationReq struct {
	ConnectorID     int    `json:"connectorId"`
	ErrorCode       string `json:"errorCode"`
	Status          string `json:"status"`
	Timestamp       string `json:"timestamp,omitempty"`
	Info            string `json:"info,omitempty"`
	VendorID        string `json:"vendorId,omitempty"`
	VendorErrorCode string `json:"vendorErrorCode,omitempty"`
}

// MeterValuesReq is the CP→CSMS MeterValues payload.
type MeterValuesReq struct {
	ConnectorID   int          `json:"connectorId"`
	TransactionID *int         `json:"transactionId,omitempty"`
	MeterValue    []MeterValue `json:"meterValue"`
}

// MeterValue is a timestamped set of sampled values.
type MeterValue struct {
	Timestamp    string         `json:"timestamp"`
	SampledValue []SampledValue `json:"sampledValue"`
}

// SampledValue is a single meter reading.
type SampledValue struct {
	Value     string `json:"value"`
	Context   string `json:"context,omitempty"`
	Format    string `json:"format,omitempty"`
	Measurand string `json:"measurand,omitempty"`
	Phase     string `json:"phase,omitempty"`
	Location  string `json:"location,omitempty"`
	Unit      string `json:"unit,omitempty"`
}

// HeartbeatConf is the CSMS→CP Heartbeat response.
type HeartbeatConf struct {
	CurrentTime time.Time `json:"currentTime"`
}

// StartTransactionReq is the CP→CSMS StartTransaction payload.
type StartTransactionReq struct {
	ConnectorID   int    `json:"connectorId"`
	IDTag         string `json:"idTag"`
	MeterStart    int    `json:"meterStart"`
	Timestamp     string `json:"timestamp"`
	ReservationID *int   `json:"reservationId,omitempty"`
}

// StartTransactionConf is the CSMS→CP response.
type StartTransactionConf struct {
	TransactionID int `json:"transactionId"`
	IDTagInfo     struct {
		Status string `json:"status"`
	} `json:"idTagInfo"`
}

// StopTransactionReq is the CP→CSMS StopTransaction payload.
type StopTransactionReq struct {
	TransactionID int    `json:"transactionId"`
	IDTag         string `json:"idTag,omitempty"`
	MeterStop     int    `json:"meterStop"`
	Timestamp     string `json:"timestamp"`
	Reason        string `json:"reason,omitempty"`
}

// StopTransactionConf is the CSMS→CP response.
type StopTransactionConf struct {
	IDTagInfo *struct {
		Status string `json:"status"`
	} `json:"idTagInfo,omitempty"`
}
