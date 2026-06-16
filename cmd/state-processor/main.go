package main

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	_ "github.com/lib/pq"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"github.com/redis/go-redis/v9"

	"github.com/spirii/ocpp-platform/internal/event"
	kafkapkg "github.com/spirii/ocpp-platform/internal/kafka"
	"github.com/spirii/ocpp-platform/internal/ocpp"
)

// Prometheus metrics
var (
	eventsProcessed = prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: "processor_events_total",
		Help: "Total events processed, by type and status.",
	}, []string{"event_type", "status"})
	dedupHits = prometheus.NewCounter(prometheus.CounterOpts{
		Name: "processor_dedup_hits_total",
		Help: "Total duplicate events skipped.",
	})
	pgWriteDuration = prometheus.NewHistogram(prometheus.HistogramOpts{
		Name:    "processor_pg_write_duration_seconds",
		Help:    "PostgreSQL write latency.",
		Buckets: []float64{0.001, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1},
	})
	redisWriteDuration = prometheus.NewHistogram(prometheus.HistogramOpts{
		Name:    "processor_redis_write_duration_seconds",
		Help:    "Redis latest-state write latency.",
		Buckets: []float64{0.0005, 0.001, 0.005, 0.01, 0.025, 0.05, 0.1},
	})
	pgWriteErrors = prometheus.NewCounter(prometheus.CounterOpts{
		Name: "processor_pg_write_errors_total",
		Help: "Total PostgreSQL write errors.",
	})
	redisWriteErrors = prometheus.NewCounter(prometheus.CounterOpts{
		Name: "processor_redis_write_errors_total",
		Help: "Total Redis write errors.",
	})
	eventLag = prometheus.NewHistogram(prometheus.HistogramOpts{
		Name:    "processor_event_lag_seconds",
		Help:    "Time between event timestamp and processing time (end-to-end lag).",
		Buckets: []float64{0.1, 0.5, 1, 2, 5, 10, 30, 60},
	})
)

func init() {
	prometheus.MustRegister(eventsProcessed, dedupHits, pgWriteDuration,
		redisWriteDuration, pgWriteErrors, redisWriteErrors, eventLag)
}

func main() {
	brokers := strings.Split(getEnv("KAFKA_BROKERS", "kafka:9092"), ",")
	pgDSN := getEnv("POSTGRES_DSN", "postgres://spirii:spirii@postgres:5432/spirii?sslmode=disable")
	redisAddr := getEnv("REDIS_ADDR", "redis:6379")
	metricsAddr := getEnv("METRICS_ADDR", ":8082")

	// Metrics HTTP server
	go func() {
		metricsMux := http.NewServeMux()
		metricsMux.Handle("/metrics", promhttp.Handler())
		metricsMux.HandleFunc("/healthz", func(w http.ResponseWriter, r *http.Request) {
			w.Write([]byte(`{"status":"ok"}`))
		})
		log.Printf("state-processor metrics on %s", metricsAddr)
		http.ListenAndServe(metricsAddr, metricsMux)
	}()

	db, err := sql.Open("postgres", pgDSN)
	if err != nil {
		log.Fatalf("postgres connect: %v", err)
	}
	defer db.Close()

	for i := 0; i < 30; i++ {
		if err := db.Ping(); err == nil {
			break
		}
		log.Printf("waiting for postgres... (%d/30)", i+1)
		time.Sleep(time.Second)
	}

	if err := createTables(db); err != nil {
		log.Fatalf("create tables: %v", err)
	}

	rdb := redis.NewClient(&redis.Options{Addr: redisAddr})
	defer rdb.Close()

	for i := 0; i < 30; i++ {
		if err := rdb.Ping(context.Background()).Err(); err == nil {
			break
		}
		log.Printf("waiting for redis... (%d/30)", i+1)
		time.Sleep(time.Second)
	}

	processor := &stateProcessor{db: db, rdb: rdb}

	consumer := kafkapkg.NewConsumer(brokers, "state-processor", kafkapkg.AllTopics())
	defer consumer.Close()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	go func() {
		quit := make(chan os.Signal, 1)
		signal.Notify(quit, syscall.SIGINT, syscall.SIGTERM)
		<-quit
		log.Println("shutting down...")
		cancel()
	}()

	log.Println("state-processor started, consuming from Kafka...")
	consumer.Run(ctx, processor.handleMessage)
}

type stateProcessor struct {
	db  *sql.DB
	rdb *redis.Client
}

func (p *stateProcessor) handleMessage(ctx context.Context, topic string, key, value []byte) error {
	var evt event.Event
	if err := json.Unmarshal(value, &evt); err != nil {
		eventsProcessed.WithLabelValues("unknown", "unmarshal_error").Inc()
		return fmt.Errorf("unmarshal event: %w", err)
	}

	// Track end-to-end lag
	lag := time.Since(evt.Timestamp).Seconds()
	if lag > 0 {
		eventLag.Observe(lag)
	}

	// Deduplicate on message_id
	deduped, err := p.deduplicate(ctx, evt.MessageID)
	if err != nil {
		eventsProcessed.WithLabelValues(evt.EventType, "dedup_error").Inc()
		return fmt.Errorf("dedup check: %w", err)
	}
	if deduped {
		dedupHits.Inc()
		eventsProcessed.WithLabelValues(evt.EventType, "duplicate").Inc()
		return nil
	}

	// Write to history (PostgreSQL)
	pgStart := time.Now()
	if err := p.writeHistory(ctx, &evt); err != nil {
		pgWriteErrors.Inc()
		eventsProcessed.WithLabelValues(evt.EventType, "pg_error").Inc()
		return fmt.Errorf("write history: %w", err)
	}
	pgWriteDuration.Observe(time.Since(pgStart).Seconds())

	// Update latest state (Redis)
	redisStart := time.Now()
	if err := p.updateLatestState(ctx, &evt); err != nil {
		redisWriteErrors.Inc()
		eventsProcessed.WithLabelValues(evt.EventType, "redis_error").Inc()
		return fmt.Errorf("update state: %w", err)
	}
	redisWriteDuration.Observe(time.Since(redisStart).Seconds())

	eventsProcessed.WithLabelValues(evt.EventType, "ok").Inc()
	return nil
}

func (p *stateProcessor) deduplicate(ctx context.Context, messageID string) (bool, error) {
	key := fmt.Sprintf("dedup:%s", messageID)
	set, err := p.rdb.SetNX(ctx, key, "1", time.Hour).Result()
	if err != nil {
		return false, err
	}
	return !set, nil
}

func (p *stateProcessor) writeHistory(ctx context.Context, evt *event.Event) error {
	_, err := p.db.ExecContext(ctx,
		`INSERT INTO event_history (event_id, message_id, charger_id, connector_id, event_type, event_timestamp, received_at, payload)
		 VALUES ($1, $2, $3, $4, $5, $6, $7, $8)
		 ON CONFLICT (event_id) DO NOTHING`,
		evt.EventID, evt.MessageID, evt.ChargerID, evt.ConnectorID,
		evt.EventType, evt.Timestamp, evt.ReceivedAt, string(evt.Payload),
	)
	return err
}

func (p *stateProcessor) updateLatestState(ctx context.Context, evt *event.Event) error {
	switch evt.EventType {
	case ocpp.ActionBootNotification:
		return p.updateBootState(ctx, evt)
	case ocpp.ActionStatusNotification:
		return p.updateConnectorStatus(ctx, evt)
	case ocpp.ActionMeterValues:
		return p.updateMeterValues(ctx, evt)
	case ocpp.ActionHeartbeat:
		return p.updateHeartbeat(ctx, evt)
	case ocpp.ActionStartTransaction, ocpp.ActionStopTransaction:
		return p.updateTransaction(ctx, evt)
	}
	return nil
}

func (p *stateProcessor) updateBootState(ctx context.Context, evt *event.Event) error {
	var req ocpp.BootNotificationReq
	if err := json.Unmarshal(evt.Payload, &req); err != nil {
		return err
	}
	chargerKey := fmt.Sprintf("charger:%s", evt.ChargerID)
	return p.rdb.HSet(ctx, chargerKey,
		"vendor", req.ChargePointVendor,
		"model", req.ChargePointModel,
		"serial", req.ChargePointSerialNumber,
		"firmware", req.FirmwareVersion,
		"online", "true",
		"last_seen", evt.Timestamp.Format(time.RFC3339),
		"last_boot", evt.Timestamp.Format(time.RFC3339),
	).Err()
}

func (p *stateProcessor) updateConnectorStatus(ctx context.Context, evt *event.Event) error {
	var req ocpp.StatusNotificationReq
	if err := json.Unmarshal(evt.Payload, &req); err != nil {
		return err
	}
	connKey := fmt.Sprintf("charger:%s:conn:%d", evt.ChargerID, req.ConnectorID)
	existing, err := p.rdb.HGet(ctx, connKey, "status_ts").Result()
	if err == nil && existing != "" {
		existingTime, _ := time.Parse(time.RFC3339, existing)
		if !evt.Timestamp.After(existingTime) {
			return nil
		}
	}
	pipe := p.rdb.Pipeline()
	pipe.HSet(ctx, connKey,
		"status", req.Status, "error_code", req.ErrorCode,
		"status_ts", evt.Timestamp.Format(time.RFC3339), "info", req.Info,
	)
	chargerKey := fmt.Sprintf("charger:%s", evt.ChargerID)
	pipe.HSet(ctx, chargerKey, "last_seen", evt.Timestamp.Format(time.RFC3339), "online", "true")
	pipe.SAdd(ctx, fmt.Sprintf("charger:%s:connectors", evt.ChargerID), fmt.Sprintf("%d", req.ConnectorID))
	pipe.SAdd(ctx, "chargers", evt.ChargerID)
	_, err = pipe.Exec(ctx)
	return err
}

func (p *stateProcessor) updateMeterValues(ctx context.Context, evt *event.Event) error {
	var req ocpp.MeterValuesReq
	if err := json.Unmarshal(evt.Payload, &req); err != nil {
		return err
	}
	if len(req.MeterValue) == 0 {
		return nil
	}
	mv := req.MeterValue[len(req.MeterValue)-1]
	connKey := fmt.Sprintf("charger:%s:conn:%d", evt.ChargerID, req.ConnectorID)
	existing, err := p.rdb.HGet(ctx, connKey, "meter_ts").Result()
	if err == nil && existing != "" {
		existingTime, _ := time.Parse(time.RFC3339, existing)
		if !evt.Timestamp.After(existingTime) {
			return nil
		}
	}
	fields := map[string]interface{}{"meter_ts": evt.Timestamp.Format(time.RFC3339)}
	for _, sv := range mv.SampledValue {
		switch sv.Measurand {
		case "Energy.Active.Import.Register":
			fields["energy_wh"] = sv.Value
		case "Power.Active.Import":
			fields["power_w"] = sv.Value
		case "SoC":
			fields["soc_percent"] = sv.Value
		case "Voltage":
			fields["voltage_v"] = sv.Value
		case "Current.Import":
			fields["current_a"] = sv.Value
		default:
			fields["meter_"+sv.Measurand] = sv.Value
		}
	}
	pipe := p.rdb.Pipeline()
	pipe.HSet(ctx, connKey, fields)
	chargerKey := fmt.Sprintf("charger:%s", evt.ChargerID)
	pipe.HSet(ctx, chargerKey, "last_seen", evt.Timestamp.Format(time.RFC3339))
	pipe.SAdd(ctx, fmt.Sprintf("charger:%s:connectors", evt.ChargerID), fmt.Sprintf("%d", req.ConnectorID))
	pipe.SAdd(ctx, "chargers", evt.ChargerID)
	_, err = pipe.Exec(ctx)
	return err
}

func (p *stateProcessor) updateHeartbeat(ctx context.Context, evt *event.Event) error {
	chargerKey := fmt.Sprintf("charger:%s", evt.ChargerID)
	return p.rdb.HSet(ctx, chargerKey, "last_seen", evt.Timestamp.Format(time.RFC3339), "online", "true").Err()
}

func (p *stateProcessor) updateTransaction(ctx context.Context, evt *event.Event) error {
	connKey := fmt.Sprintf("charger:%s:conn:%d", evt.ChargerID, evt.ConnectorID)
	chargerKey := fmt.Sprintf("charger:%s", evt.ChargerID)
	pipe := p.rdb.Pipeline()
	if evt.EventType == ocpp.ActionStartTransaction {
		var req ocpp.StartTransactionReq
		json.Unmarshal(evt.Payload, &req)
		pipe.HSet(ctx, connKey, "active_transaction", "true", "tx_id_tag", req.IDTag)
		pipe.SAdd(ctx, fmt.Sprintf("charger:%s:connectors", evt.ChargerID), fmt.Sprintf("%d", req.ConnectorID))
	} else {
		pipe.HSet(ctx, connKey, "active_transaction", "false")
	}
	pipe.HSet(ctx, chargerKey, "last_seen", evt.Timestamp.Format(time.RFC3339))
	pipe.SAdd(ctx, "chargers", evt.ChargerID)
	_, err := pipe.Exec(ctx)
	return err
}

func createTables(db *sql.DB) error {
	_, err := db.Exec(`
		CREATE TABLE IF NOT EXISTS event_history (
			event_id        TEXT PRIMARY KEY,
			message_id      TEXT NOT NULL,
			charger_id      TEXT NOT NULL,
			connector_id    INTEGER NOT NULL,
			event_type      TEXT NOT NULL,
			event_timestamp TIMESTAMPTZ NOT NULL,
			received_at     TIMESTAMPTZ NOT NULL,
			payload         JSONB NOT NULL,
			created_at      TIMESTAMPTZ DEFAULT NOW()
		);
		CREATE INDEX IF NOT EXISTS idx_event_history_charger ON event_history (charger_id, event_timestamp DESC);
		CREATE INDEX IF NOT EXISTS idx_event_history_type ON event_history (event_type, event_timestamp DESC);
		CREATE INDEX IF NOT EXISTS idx_event_history_message ON event_history (message_id);
	`)
	return err
}

func getEnv(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}
