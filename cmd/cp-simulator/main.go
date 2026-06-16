package main

import (
	"encoding/json"
	"fmt"
	"log"
	"math/rand"
	"os"
	"strconv"
	"sync"
	"time"

	"github.com/google/uuid"
	"github.com/gorilla/websocket"

	"github.com/spirii/ocpp-platform/internal/ocpp"
)

func main() {
	wsURL := getEnv("WSGW_URL", "ws://wsgw:8080")
	numChargers, _ := strconv.Atoi(getEnv("NUM_CHARGERS", "5"))
	connectorsPerCharger, _ := strconv.Atoi(getEnv("CONNECTORS_PER_CHARGER", "2"))
	meterIntervalSec, _ := strconv.Atoi(getEnv("METER_INTERVAL_SEC", "15"))
	heartbeatIntervalSec, _ := strconv.Atoi(getEnv("HEARTBEAT_INTERVAL_SEC", "60"))

	log.Printf("Starting CP simulator: %d chargers, %d connectors each, meter interval %ds",
		numChargers, connectorsPerCharger, meterIntervalSec)

	var wg sync.WaitGroup
	for i := 1; i <= numChargers; i++ {
		wg.Add(1)
		cpID := fmt.Sprintf("SPI-%05d", i)
		go func(id string) {
			defer wg.Done()
			runCharger(id, wsURL, connectorsPerCharger, meterIntervalSec, heartbeatIntervalSec)
		}(cpID)
		// Stagger connections to avoid thundering herd
		time.Sleep(time.Duration(100+rand.Intn(200)) * time.Millisecond)
	}
	wg.Wait()
}

func runCharger(cpID, wsURL string, numConnectors, meterIntervalSec, heartbeatIntervalSec int) {
	for {
		if err := chargerSession(cpID, wsURL, numConnectors, meterIntervalSec, heartbeatIntervalSec); err != nil {
			log.Printf("[%s] session error: %v, reconnecting in 5s...", cpID, err)
			time.Sleep(5 * time.Second)
		}
	}
}

func chargerSession(cpID, wsURL string, numConnectors, meterIntervalSec, heartbeatIntervalSec int) error {
	url := fmt.Sprintf("%s/ocpp/1.6/%s", wsURL, cpID)
	conn, _, err := websocket.DefaultDialer.Dial(url, nil)
	if err != nil {
		return fmt.Errorf("dial: %w", err)
	}
	defer conn.Close()

	log.Printf("[%s] connected", cpID)

	// Start response reader
	responses := make(chan []byte, 100)
	go func() {
		for {
			_, data, err := conn.ReadMessage()
			if err != nil {
				close(responses)
				return
			}
			responses <- data
		}
	}()

	// BootNotification
	if err := sendAndWait(conn, responses, ocpp.ActionBootNotification, ocpp.BootNotificationReq{
		ChargePointVendor:       "Spirii",
		ChargePointModel:        "S3-50kW",
		ChargePointSerialNumber: cpID,
		FirmwareVersion:         "3.2.1",
	}); err != nil {
		return fmt.Errorf("boot: %w", err)
	}
	log.Printf("[%s] boot accepted", cpID)

	// Set all connectors to Available
	for c := 1; c <= numConnectors; c++ {
		sendAndWait(conn, responses, ocpp.ActionStatusNotification, ocpp.StatusNotificationReq{
			ConnectorID: c,
			ErrorCode:   ocpp.ErrorNoError,
			Status:      ocpp.StatusAvailable,
			Timestamp:   time.Now().UTC().Format(time.RFC3339),
		})
	}

	// Main loop: simulate charging sessions
	heartbeatTicker := time.NewTicker(time.Duration(heartbeatIntervalSec) * time.Second)
	defer heartbeatTicker.Stop()

	for {
		// Pick a random connector for a charging session
		connectorID := 1 + rand.Intn(numConnectors)
		if err := simulateSession(conn, responses, cpID, connectorID, meterIntervalSec, heartbeatTicker); err != nil {
			return err
		}

		// Wait before next session
		idleTime := 5 + rand.Intn(10)
		time.Sleep(time.Duration(idleTime) * time.Second)
	}
}

func simulateSession(conn *websocket.Conn, responses chan []byte, cpID string, connectorID, meterIntervalSec int, heartbeatTicker *time.Ticker) error {
	now := func() string { return time.Now().UTC().Format(time.RFC3339) }

	// Preparing
	if err := sendAndWait(conn, responses, ocpp.ActionStatusNotification, ocpp.StatusNotificationReq{
		ConnectorID: connectorID,
		ErrorCode:   ocpp.ErrorNoError,
		Status:      ocpp.StatusPreparing,
		Timestamp:   now(),
	}); err != nil {
		return err
	}

	time.Sleep(time.Duration(1+rand.Intn(3)) * time.Second)

	// StartTransaction
	meterStart := rand.Intn(100000)
	if err := sendAndWait(conn, responses, ocpp.ActionStartTransaction, ocpp.StartTransactionReq{
		ConnectorID: connectorID,
		IDTag:       fmt.Sprintf("RFID-%s", uuid.New().String()[:8]),
		MeterStart:  meterStart,
		Timestamp:   now(),
	}); err != nil {
		return err
	}

	// Charging
	if err := sendAndWait(conn, responses, ocpp.ActionStatusNotification, ocpp.StatusNotificationReq{
		ConnectorID: connectorID,
		ErrorCode:   ocpp.ErrorNoError,
		Status:      ocpp.StatusCharging,
		Timestamp:   now(),
	}); err != nil {
		return err
	}

	// Send MeterValues during charging (3-8 readings)
	numReadings := 3 + rand.Intn(6)
	energy := meterStart
	soc := 20 + rand.Intn(30) // starting SoC
	for i := 0; i < numReadings; i++ {
		// Check for heartbeat
		select {
		case <-heartbeatTicker.C:
			sendAndWait(conn, responses, ocpp.ActionHeartbeat, struct{}{})
		default:
		}

		energy += 500 + rand.Intn(2000)    // Wh increment
		power := 10000 + rand.Intn(40000)  // W
		soc += 2 + rand.Intn(5)
		if soc > 100 {
			soc = 100
		}

		if err := sendAndWait(conn, responses, ocpp.ActionMeterValues, ocpp.MeterValuesReq{
			ConnectorID: connectorID,
			MeterValue: []ocpp.MeterValue{
				{
					Timestamp: now(),
					SampledValue: []ocpp.SampledValue{
						{Value: strconv.Itoa(energy), Measurand: "Energy.Active.Import.Register", Unit: "Wh", Context: "Sample.Periodic"},
						{Value: strconv.Itoa(power), Measurand: "Power.Active.Import", Unit: "W", Context: "Sample.Periodic"},
						{Value: strconv.Itoa(soc), Measurand: "SoC", Unit: "Percent", Context: "Sample.Periodic"},
					},
				},
			},
		}); err != nil {
			return err
		}

		time.Sleep(time.Duration(meterIntervalSec) * time.Second)
	}

	// Finishing
	sendAndWait(conn, responses, ocpp.ActionStatusNotification, ocpp.StatusNotificationReq{
		ConnectorID: connectorID,
		ErrorCode:   ocpp.ErrorNoError,
		Status:      ocpp.StatusFinishing,
		Timestamp:   now(),
	})

	// StopTransaction
	sendAndWait(conn, responses, ocpp.ActionStopTransaction, ocpp.StopTransactionReq{
		TransactionID: 0, // we don't track the tx ID in this simulator
		MeterStop:     energy,
		Timestamp:     now(),
		Reason:        "EVDisconnected",
	})

	// Back to Available
	sendAndWait(conn, responses, ocpp.ActionStatusNotification, ocpp.StatusNotificationReq{
		ConnectorID: connectorID,
		ErrorCode:   ocpp.ErrorNoError,
		Status:      ocpp.StatusAvailable,
		Timestamp:   now(),
	})

	log.Printf("[%s] connector %d session complete, energy=%d Wh", cpID, connectorID, energy)
	return nil
}

func sendAndWait(conn *websocket.Conn, responses chan []byte, action string, payload interface{}) error {
	msgID := uuid.New().String()[:8]
	data, err := ocpp.MakeCall(msgID, action, payload)
	if err != nil {
		return fmt.Errorf("marshal %s: %w", action, err)
	}
	if err := conn.WriteMessage(websocket.TextMessage, data); err != nil {
		return fmt.Errorf("write %s: %w", action, err)
	}

	// Wait for response (with timeout)
	select {
	case resp, ok := <-responses:
		if !ok {
			return fmt.Errorf("connection closed")
		}
		// Parse to verify it's a CALLRESULT for our messageID
		msg, err := ocpp.ParseMessage(resp)
		if err != nil {
			return fmt.Errorf("parse response: %w", err)
		}
		if msg.TypeID == ocpp.MessageTypeCallError {
			return fmt.Errorf("CALLERROR for %s: %s", action, string(msg.Payload))
		}
		_ = msg // could extract transactionId from StartTransaction response
		return nil
	case <-time.After(10 * time.Second):
		return fmt.Errorf("timeout waiting for %s response", action)
	}
}

func getEnv(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}

// Helper to unmarshal JSON responses when needed
func unmarshalResponse(data json.RawMessage, v interface{}) error {
	return json.Unmarshal(data, v)
}
