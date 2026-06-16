package kafka

// Topic names matching OCPP message types.
const (
	TopicBootNotification   = "ocpp.boot_notification"
	TopicStatusNotification = "ocpp.status_notification"
	TopicMeterValues        = "ocpp.meter_values"
	TopicTransactions       = "ocpp.transactions"
	TopicHeartbeat          = "ocpp.heartbeat"
)

// AllTopics returns all OCPP Kafka topics.
func AllTopics() []string {
	return []string{
		TopicBootNotification,
		TopicStatusNotification,
		TopicMeterValues,
		TopicTransactions,
		TopicHeartbeat,
	}
}
