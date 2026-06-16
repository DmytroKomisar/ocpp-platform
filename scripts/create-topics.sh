#!/bin/bash
# Create Kafka topics for OCPP telemetry pipeline
KAFKA_BIN="/opt/kafka/bin"
BOOTSTRAP="localhost:9092"

for TOPIC in ocpp.boot_notification ocpp.status_notification ocpp.meter_values ocpp.transactions ocpp.heartbeat; do
  echo "Creating topic: $TOPIC"
  $KAFKA_BIN/kafka-topics.sh --create \
    --bootstrap-server $BOOTSTRAP \
    --topic "$TOPIC" \
    --partitions 6 \
    --replication-factor 1 \
    --if-not-exists 2>&1 || true
done

echo "Listing topics:"
$KAFKA_BIN/kafka-topics.sh --list --bootstrap-server $BOOTSTRAP
