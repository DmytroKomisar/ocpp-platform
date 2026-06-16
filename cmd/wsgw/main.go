package main

import (
	"context"
	"encoding/json"
	"log"
	"net/http"
	"os"
	"os/signal"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/google/uuid"
	"github.com/gorilla/websocket"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"

	"github.com/spirii/ocpp-platform/internal/event"
	kafkapkg "github.com/spirii/ocpp-platform/internal/kafka"
	"github.com/spirii/ocpp-platform/internal/ocpp"
)

var upgrader = websocket.Upgrader{
	CheckOrigin: func(r *http.Request) bool { return true },
}

var txCounter int64

// Prometheus metrics
var (
	wsConnections = prometheus.NewGauge(prometheus.GaugeOpts{
		Name: "wsgw_active_connections",
		Help: "Number of active WebSocket connections from charge points.",
	})
	ocppMessagesTotal = prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: "wsgw_ocpp_messages_total",
		Help: "Total OCPP messages received, by action.",
	}, []string{"action"})
	kafkaPublishTotal = prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: "wsgw_kafka_publish_total",
		Help: "Total Kafka publish attempts, by topic and status.",
	}, []string{"topic", "status"})
	kafkaPublishDuration = prometheus.NewHistogramVec(prometheus.HistogramOpts{
		Name:    "wsgw_kafka_publish_duration_seconds",
		Help:    "Kafka publish latency in seconds.",
		Buckets: []float64{0.001, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1},
	}, []string{"topic"})
	wsConnectionsTotal = prometheus.NewCounter(prometheus.CounterOpts{
		Name: "wsgw_connections_total",
		Help: "Total WebSocket connections accepted since start.",
	})
	ocppParseErrors = prometheus.NewCounter(prometheus.CounterOpts{
		Name: "wsgw_ocpp_parse_errors_total",
		Help: "Total OCPP message parse errors.",
	})
)

func init() {
	prometheus.MustRegister(wsConnections, ocppMessagesTotal, kafkaPublishTotal,
		kafkaPublishDuration, wsConnectionsTotal, ocppParseErrors)
}

func main() {
	brokers := strings.Split(getEnv("KAFKA_BROKERS", "kafka:9092"), ",")
	listenAddr := getEnv("LISTEN_ADDR", ":8080")

	producer := kafkapkg.NewProducer(brokers, kafkapkg.AllTopics())
	defer producer.Close()

	gw := &gateway{producer: producer}

	mux := http.NewServeMux()
	mux.HandleFunc("/ocpp/1.6/", gw.handleOCPP)
	mux.HandleFunc("/healthz", func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		w.Write([]byte(`{"status":"ok"}`))
	})
	mux.Handle("/metrics", promhttp.Handler())

	server := &http.Server{Addr: listenAddr, Handler: mux}

	go func() {
		log.Printf("WSGW listening on %s", listenAddr)
		if err := server.ListenAndServe(); err != http.ErrServerClosed {
			log.Fatalf("server error: %v", err)
		}
	}()

	quit := make(chan os.Signal, 1)
	signal.Notify(quit, syscall.SIGINT, syscall.SIGTERM)
	<-quit
	log.Println("shutting down...")
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	server.Shutdown(ctx)
}

type gateway struct {
	producer *kafkapkg.Producer
	mu       sync.RWMutex
	conns    map[string]*websocket.Conn
}

func (gw *gateway) handleOCPP(w http.ResponseWriter, r *http.Request) {
	path := strings.TrimPrefix(r.URL.Path, "/ocpp/1.6/")
	cpID := strings.TrimRight(path, "/")
	if cpID == "" {
		http.Error(w, "missing chargePointId in URL", http.StatusBadRequest)
		return
	}

	conn, err := upgrader.Upgrade(w, r, nil)
	if err != nil {
		log.Printf("upgrade error for %s: %v", cpID, err)
		return
	}
	defer conn.Close()

	wsConnections.Inc()
	wsConnectionsTotal.Inc()
	defer wsConnections.Dec()

	log.Printf("CP connected: %s", cpID)

	for {
		_, data, err := conn.ReadMessage()
		if err != nil {
			if websocket.IsUnexpectedCloseError(err, websocket.CloseGoingAway, websocket.CloseNormalClosure) {
				log.Printf("CP %s read error: %v", cpID, err)
			}
			log.Printf("CP disconnected: %s", cpID)
			return
		}

		msg, err := ocpp.ParseMessage(data)
		if err != nil {
			log.Printf("CP %s parse error: %v", cpID, err)
			ocppParseErrors.Inc()
			continue
		}

		if msg.TypeID != ocpp.MessageTypeCall {
			continue
		}

		ocppMessagesTotal.WithLabelValues(msg.Action).Inc()

		response, topic, err := gw.processCall(cpID, msg)
		if err != nil {
			log.Printf("CP %s process error for %s: %v", cpID, msg.Action, err)
			continue
		}

		respBytes, err := ocpp.MakeCallResult(msg.MessageID, response)
		if err != nil {
			log.Printf("CP %s marshal error: %v", cpID, err)
			continue
		}
		if err := conn.WriteMessage(websocket.TextMessage, respBytes); err != nil {
			log.Printf("CP %s write error: %v", cpID, err)
			return
		}

		if topic != "" {
			evt := gw.makeEvent(cpID, msg)
			evtBytes, _ := json.Marshal(evt)
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			start := time.Now()
			if err := gw.producer.Publish(ctx, topic, cpID, evtBytes); err != nil {
				log.Printf("kafka publish error for %s/%s: %v", cpID, msg.Action, err)
				kafkaPublishTotal.WithLabelValues(topic, "error").Inc()
			} else {
				kafkaPublishTotal.WithLabelValues(topic, "ok").Inc()
			}
			kafkaPublishDuration.WithLabelValues(topic).Observe(time.Since(start).Seconds())
			cancel()
		}
	}
}

func (gw *gateway) processCall(cpID string, msg *ocpp.Message) (interface{}, string, error) {
	switch msg.Action {
	case ocpp.ActionBootNotification:
		return ocpp.BootNotificationConf{
			Status:      "Accepted",
			CurrentTime: time.Now().UTC(),
			Interval:    60,
		}, kafkapkg.TopicBootNotification, nil

	case ocpp.ActionStatusNotification:
		return struct{}{}, kafkapkg.TopicStatusNotification, nil

	case ocpp.ActionMeterValues:
		return struct{}{}, kafkapkg.TopicMeterValues, nil

	case ocpp.ActionHeartbeat:
		return ocpp.HeartbeatConf{
			CurrentTime: time.Now().UTC(),
		}, kafkapkg.TopicHeartbeat, nil

	case ocpp.ActionStartTransaction:
		txID := int(atomic.AddInt64(&txCounter, 1))
		return ocpp.StartTransactionConf{
			TransactionID: txID,
			IDTagInfo:     struct{ Status string `json:"status"` }{Status: "Accepted"},
		}, kafkapkg.TopicTransactions, nil

	case ocpp.ActionStopTransaction:
		return ocpp.StopTransactionConf{}, kafkapkg.TopicTransactions, nil

	default:
		log.Printf("CP %s: unsupported action %s", cpID, msg.Action)
		return struct{}{}, "", nil
	}
}

func (gw *gateway) makeEvent(cpID string, msg *ocpp.Message) event.Event {
	connectorID := 0
	var payload struct {
		ConnectorID int `json:"connectorId"`
	}
	json.Unmarshal(msg.Payload, &payload)
	connectorID = payload.ConnectorID

	var ts time.Time
	var tsPayload struct {
		Timestamp string `json:"timestamp"`
	}
	if err := json.Unmarshal(msg.Payload, &tsPayload); err == nil && tsPayload.Timestamp != "" {
		if parsed, err := time.Parse(time.RFC3339, tsPayload.Timestamp); err == nil {
			ts = parsed
		}
	}
	if ts.IsZero() {
		ts = time.Now().UTC()
	}

	return event.Event{
		EventID:     uuid.New().String(),
		MessageID:   msg.MessageID,
		ChargerID:   cpID,
		ConnectorID: connectorID,
		EventType:   msg.Action,
		Timestamp:   ts,
		ReceivedAt:  time.Now().UTC(),
		Payload:     msg.Payload,
	}
}

func getEnv(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}
