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
	numChargers, _ := strconv.Atoi(getEnv("NUM_CHARGERS", "50"))
	connectorsPerCharger, _ := strconv.Atoi(getEnv("CONNECTORS_PER_CHARGER", "2"))
	meterIntervalSec, _ := strconv.Atoi(getEnv("METER_INTERVAL_SEC", "10"))
	heartbeatIntervalSec, _ := strconv.Atoi(getEnv("HEARTBEAT_INTERVAL_SEC", "30"))

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

// chargerModels adds variety to the fleet
var chargerModels = []struct {
	vendor   string
	model    string
	firmware string
	maxPower int // max watts
}{
	{"Spirii", "S3-50kW", "3.2.1", 50000},
	{"Spirii", "S3-50kW", "3.2.1", 50000},
	{"Spirii", "S5-150kW", "2.1.0", 150000},
	{"ABB", "Terra 54", "1.8.3", 50000},
	{"ABB", "Terra 124", "2.0.1", 120000},
	{"Kempower", "S-Series", "4.1.2", 40000},
	{"Alfen", "Eve Single S-line", "3.5.0", 22000},
	{"Alfen", "Eve Double Pro-line", "3.5.0", 22000},
	{"Easee", "Charge", "5.0.3", 22000},
	{"Zaptec", "Go", "2.3.1", 22000},
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

	// Pick a charger model based on cpID hash for consistency
	modelIdx := hashID(cpID) % len(chargerModels)
	m := chargerModels[modelIdx]

	// BootNotification
	if err := sendAndWait(conn, responses, ocpp.ActionBootNotification, ocpp.BootNotificationReq{
		ChargePointVendor:       m.vendor,
		ChargePointModel:        m.model,
		ChargePointSerialNumber: cpID,
		FirmwareVersion:         m.firmware,
	}); err != nil {
		return fmt.Errorf("boot: %w", err)
	}
	log.Printf("[%s] boot accepted (%s %s)", cpID, m.vendor, m.model)

	// Set all connectors to Available
	for c := 1; c <= numConnectors; c++ {
		sendAndWait(conn, responses, ocpp.ActionStatusNotification, ocpp.StatusNotificationReq{
			ConnectorID: c,
			ErrorCode:   ocpp.ErrorNoError,
			Status:      ocpp.StatusAvailable,
			Timestamp:   time.Now().UTC().Format(time.RFC3339),
		})
	}

	// Main loop: realistic idle/charge cycle
	heartbeatTicker := time.NewTicker(time.Duration(heartbeatIntervalSec) * time.Second)
	defer heartbeatTicker.Stop()

	for {
		// Each connector independently decides whether to start a session
		connectorID := 1 + rand.Intn(numConnectors)

		// ~25% chance a session starts on this cycle (most chargers idle most of the time)
		if rand.Float64() < 0.25 {
			if err := simulateSession(conn, responses, cpID, connectorID, meterIntervalSec, m.maxPower, heartbeatTicker); err != nil {
				return err
			}
		}

		// Idle period between session attempts: 30s - 3min (realistic)
		idleTime := 30 + rand.Intn(150)

		// During idle, keep sending heartbeats
		idleEnd := time.Now().Add(time.Duration(idleTime) * time.Second)
		for time.Now().Before(idleEnd) {
			select {
			case <-heartbeatTicker.C:
				if err := sendAndWait(conn, responses, ocpp.ActionHeartbeat, struct{}{}); err != nil {
					return err
				}
			case <-time.After(5 * time.Second):
				// just a tick to recheck
			}
		}
	}
}

func simulateSession(conn *websocket.Conn, responses chan []byte, cpID string, connectorID, meterIntervalSec, maxPower int, heartbeatTicker *time.Ticker) error {
	now := func() string { return time.Now().UTC().Format(time.RFC3339) }

	// Preparing (cable plugged in, 2-8s wait)
	if err := sendAndWait(conn, responses, ocpp.ActionStatusNotification, ocpp.StatusNotificationReq{
		ConnectorID: connectorID,
		ErrorCode:   ocpp.ErrorNoError,
		Status:      ocpp.StatusPreparing,
		Timestamp:   now(),
	}); err != nil {
		return err
	}

	prepTime := 2 + rand.Intn(6)
	time.Sleep(time.Duration(prepTime) * time.Second)

	// ~5% chance: driver plugged in but walked away (stays in Preparing for a while, then Available)
	if rand.Float64() < 0.05 {
		log.Printf("[%s] conn %d: driver didn't authorize, returning to Available", cpID, connectorID)
		time.Sleep(time.Duration(15+rand.Intn(30)) * time.Second)
		return sendAndWait(conn, responses, ocpp.ActionStatusNotification, ocpp.StatusNotificationReq{
			ConnectorID: connectorID,
			ErrorCode:   ocpp.ErrorNoError,
			Status:      ocpp.StatusAvailable,
			Timestamp:   now(),
		})
	}

	// StartTransaction
	meterStart := rand.Intn(50000)
	idTag := fmt.Sprintf("RFID-%s", uuid.New().String()[:8])
	if err := sendAndWait(conn, responses, ocpp.ActionStartTransaction, ocpp.StartTransactionReq{
		ConnectorID: connectorID,
		IDTag:       idTag,
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

	// Charging duration: 5-25 meter readings (50s to ~4min at 10s interval for demo speed)
	// In reality this would be 15min-2hrs but we compress time for the demo
	numReadings := 5 + rand.Intn(20)
	energy := meterStart
	startSoC := 10 + rand.Intn(40) // 10-50% starting SoC
	soc := startSoC

	// Power curve: starts high, gradually decreases as battery fills (realistic)
	basePower := maxPower/2 + rand.Intn(maxPower/2) // 50-100% of max
	var maxPowerSeen int

	for i := 0; i < numReadings; i++ {
		// Check for heartbeat
		select {
		case <-heartbeatTicker.C:
			sendAndWait(conn, responses, ocpp.ActionHeartbeat, struct{}{})
		default:
		}

		// Power tapers as SoC increases (realistic charging curve)
		socFactor := 1.0 - float64(soc-startSoC)/float64(100-startSoC)*0.6
		if socFactor < 0.2 {
			socFactor = 0.2
		}
		power := int(float64(basePower)*socFactor) + rand.Intn(2000) - 1000 // +/- 1kW jitter
		if power < 1000 {
			power = 1000
		}
		if power > maxPowerSeen {
			maxPowerSeen = power
		}

		// Energy increment based on actual power and interval
		energyIncrement := power * meterIntervalSec / 3600 // Wh = W * s / 3600
		energyIncrement += rand.Intn(100) - 50             // small jitter
		if energyIncrement < 10 {
			energyIncrement = 10
		}
		energy += energyIncrement

		// SoC increment (slower as battery fills)
		socInc := 1 + rand.Intn(3)
		if soc > 80 {
			socInc = 1 // trickle at high SoC
		}
		soc += socInc
		if soc > 100 {
			soc = 100
		}

		// ~3% chance of SuspendedEV (vehicle paused charging temporarily)
		if rand.Float64() < 0.03 && i < numReadings-2 {
			sendAndWait(conn, responses, ocpp.ActionStatusNotification, ocpp.StatusNotificationReq{
				ConnectorID: connectorID,
				ErrorCode:   ocpp.ErrorNoError,
				Status:      ocpp.StatusSuspendedEV,
				Timestamp:   now(),
			})
			suspendTime := 5 + rand.Intn(15)
			time.Sleep(time.Duration(suspendTime) * time.Second)
			sendAndWait(conn, responses, ocpp.ActionStatusNotification, ocpp.StatusNotificationReq{
				ConnectorID: connectorID,
				ErrorCode:   ocpp.ErrorNoError,
				Status:      ocpp.StatusCharging,
				Timestamp:   now(),
			})
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

	// Finishing — cable still plugged in for a bit
	sendAndWait(conn, responses, ocpp.ActionStatusNotification, ocpp.StatusNotificationReq{
		ConnectorID: connectorID,
		ErrorCode:   ocpp.ErrorNoError,
		Status:      ocpp.StatusFinishing,
		Timestamp:   now(),
	})

	// Driver takes 5-30s to unplug (some linger longer)
	unplugDelay := 5 + rand.Intn(25)
	time.Sleep(time.Duration(unplugDelay) * time.Second)

	// Pick a stop reason
	reasons := []string{"EVDisconnected", "EVDisconnected", "EVDisconnected", "Local", "Remote"}
	reason := reasons[rand.Intn(len(reasons))]

	// StopTransaction
	sendAndWait(conn, responses, ocpp.ActionStopTransaction, ocpp.StopTransactionReq{
		TransactionID: 0,
		IDTag:         idTag,
		MeterStop:     energy,
		Timestamp:     now(),
		Reason:        reason,
	})

	// Back to Available
	sendAndWait(conn, responses, ocpp.ActionStatusNotification, ocpp.StatusNotificationReq{
		ConnectorID: connectorID,
		ErrorCode:   ocpp.ErrorNoError,
		Status:      ocpp.StatusAvailable,
		Timestamp:   now(),
	})

	totalEnergy := energy - meterStart
	log.Printf("[%s] conn %d session complete: %d Wh, SoC %d%%->%d%%, peak %d W, reason=%s",
		cpID, connectorID, totalEnergy, startSoC, soc, maxPowerSeen, reason)
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
		msg, err := ocpp.ParseMessage(resp)
		if err != nil {
			return fmt.Errorf("parse response: %w", err)
		}
		if msg.TypeID == ocpp.MessageTypeCallError {
			return fmt.Errorf("CALLERROR for %s: %s", action, string(msg.Payload))
		}
		_ = msg
		return nil
	case <-time.After(10 * time.Second):
		return fmt.Errorf("timeout waiting for %s response", action)
	}
}

func hashID(s string) int {
	h := 0
	for _, c := range s {
		h = h*31 + int(c)
	}
	if h < 0 {
		h = -h
	}
	return h
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
