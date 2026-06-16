package main

import (
	"context"
	"embed"
	"io/fs"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"os"
	"os/signal"
	"sort"
	"strings"
	"syscall"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"github.com/redis/go-redis/v9"
)

//go:embed swagger/openapi.json
var openapiSpec []byte

//go:embed dashboard/*
var dashboardFS embed.FS

// Prometheus metrics
var (
	apiRequestsTotal = prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: "api_requests_total",
		Help: "Total API requests, by endpoint and status code.",
	}, []string{"endpoint", "status"})
	apiRequestDuration = prometheus.NewHistogramVec(prometheus.HistogramOpts{
		Name:    "api_request_duration_seconds",
		Help:    "API request latency in seconds.",
		Buckets: []float64{0.001, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1},
	}, []string{"endpoint"})
	apiChargersReturned = prometheus.NewHistogram(prometheus.HistogramOpts{
		Name:    "api_chargers_returned",
		Help:    "Number of chargers returned per list request.",
		Buckets: []float64{1, 5, 10, 25, 50, 100, 500, 1000},
	})
)

func init() {
	prometheus.MustRegister(apiRequestsTotal, apiRequestDuration, apiChargersReturned)
}

func main() {
	redisAddr := getEnv("REDIS_ADDR", "redis:6379")
	listenAddr := getEnv("LISTEN_ADDR", ":8081")

	rdb := redis.NewClient(&redis.Options{Addr: redisAddr})
	defer rdb.Close()

	for i := 0; i < 30; i++ {
		if err := rdb.Ping(context.Background()).Err(); err == nil {
			break
		}
		log.Printf("waiting for redis... (%d/30)", i+1)
		time.Sleep(time.Second)
	}

	api := &apiServer{rdb: rdb}

	mux := http.NewServeMux()
	mux.HandleFunc("/chargers", api.listChargers)
	mux.HandleFunc("/chargers/", api.getChargerState)
	mux.HandleFunc("/healthz", func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		w.Write([]byte(`{"status":"ok"}`))
	})
	mux.Handle("/metrics", promhttp.Handler())
	mux.HandleFunc("/swagger/openapi.json", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.Write(openapiSpec)
	})
	mux.HandleFunc("/swagger/", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/html")
		w.Write([]byte(swaggerHTML))
	})

	// Dashboard (fleet monitoring portal)
	dashboardSub, _ := fs.Sub(dashboardFS, "dashboard")
	mux.HandleFunc("/dashboard/", func(w http.ResponseWriter, r *http.Request) {
		path := strings.TrimPrefix(r.URL.Path, "/dashboard/")
		// Serve static assets directly from embedded FS
		if strings.HasSuffix(path, ".css") || strings.HasSuffix(path, ".js") {
			data, err := fs.ReadFile(dashboardSub, path)
			if err != nil {
				http.NotFound(w, r)
				return
			}
			if strings.HasSuffix(path, ".css") {
				w.Header().Set("Content-Type", "text/css")
			} else {
				w.Header().Set("Content-Type", "application/javascript")
			}
			w.Write(data)
			return
		}
		// Everything else gets index.html (SPA routing)
		data, _ := fs.ReadFile(dashboardSub, "index.html")
		w.Header().Set("Content-Type", "text/html")
		w.Write(data)
	})

	server := &http.Server{Addr: listenAddr, Handler: mux}

	go func() {
		log.Printf("API server listening on %s", listenAddr)
		if err := server.ListenAndServe(); err != http.ErrServerClosed {
			log.Fatalf("server error: %v", err)
		}
	}()

	quit := make(chan os.Signal, 1)
	signal.Notify(quit, syscall.SIGINT, syscall.SIGTERM)
	<-quit
	log.Println("shutting down...")
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	server.Shutdown(ctx)
}

type apiServer struct {
	rdb *redis.Client
}

type ChargerState struct {
	ChargerID  string           `json:"charger_id"`
	Vendor     string           `json:"vendor,omitempty"`
	Model      string           `json:"model,omitempty"`
	Serial     string           `json:"serial,omitempty"`
	Firmware   string           `json:"firmware,omitempty"`
	Online     bool             `json:"online"`
	LastSeen   string           `json:"last_seen,omitempty"`
	LastBoot   string           `json:"last_boot,omitempty"`
	Connectors []ConnectorState `json:"connectors,omitempty"`
}

type ConnectorState struct {
	ConnectorID       int    `json:"connector_id"`
	Status            string `json:"status,omitempty"`
	ErrorCode         string `json:"error_code,omitempty"`
	StatusTimestamp    string `json:"status_timestamp,omitempty"`
	ActiveTransaction bool   `json:"active_transaction"`
	EnergyWh          string `json:"energy_wh,omitempty"`
	PowerW            string `json:"power_w,omitempty"`
	SoCPercent        string `json:"soc_percent,omitempty"`
	MeterTimestamp    string `json:"meter_timestamp,omitempty"`
}

func (a *apiServer) listChargers(w http.ResponseWriter, r *http.Request) {
	start := time.Now()
	if r.Method != http.MethodGet {
		apiRequestsTotal.WithLabelValues("list_chargers", "405").Inc()
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}

	ctx := r.Context()
	chargerIDs, err := a.rdb.SMembers(ctx, "chargers").Result()
	if err != nil {
		apiRequestsTotal.WithLabelValues("list_chargers", "500").Inc()
		http.Error(w, "redis error", http.StatusInternalServerError)
		return
	}

	sort.Strings(chargerIDs)

	chargers := make([]ChargerState, 0, len(chargerIDs))
	for _, id := range chargerIDs {
		state, err := a.buildChargerState(ctx, id)
		if err != nil {
			log.Printf("error building state for %s: %v", id, err)
			continue
		}
		chargers = append(chargers, *state)
	}

	apiChargersReturned.Observe(float64(len(chargers)))
	apiRequestsTotal.WithLabelValues("list_chargers", "200").Inc()
	apiRequestDuration.WithLabelValues("list_chargers").Observe(time.Since(start).Seconds())

	writeJSON(w, http.StatusOK, map[string]interface{}{
		"count":    len(chargers),
		"chargers": chargers,
	})
}

func (a *apiServer) getChargerState(w http.ResponseWriter, r *http.Request) {
	start := time.Now()
	if r.Method != http.MethodGet {
		apiRequestsTotal.WithLabelValues("get_charger", "405").Inc()
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}

	path := strings.TrimPrefix(r.URL.Path, "/chargers/")
	path = strings.TrimSuffix(path, "/state")
	path = strings.TrimRight(path, "/")
	chargerID := path

	if chargerID == "" {
		apiRequestsTotal.WithLabelValues("get_charger", "400").Inc()
		http.Error(w, "missing charger ID", http.StatusBadRequest)
		return
	}

	ctx := r.Context()
	exists, _ := a.rdb.SIsMember(ctx, "chargers", chargerID).Result()
	if !exists {
		apiRequestsTotal.WithLabelValues("get_charger", "404").Inc()
		apiRequestDuration.WithLabelValues("get_charger").Observe(time.Since(start).Seconds())
		writeJSON(w, http.StatusNotFound, map[string]string{"error": "charger not found"})
		return
	}

	state, err := a.buildChargerState(ctx, chargerID)
	if err != nil {
		apiRequestsTotal.WithLabelValues("get_charger", "500").Inc()
		http.Error(w, "internal error", http.StatusInternalServerError)
		return
	}

	apiRequestsTotal.WithLabelValues("get_charger", "200").Inc()
	apiRequestDuration.WithLabelValues("get_charger").Observe(time.Since(start).Seconds())

	writeJSON(w, http.StatusOK, state)
}

func (a *apiServer) buildChargerState(ctx context.Context, chargerID string) (*ChargerState, error) {
	chargerKey := fmt.Sprintf("charger:%s", chargerID)
	info, err := a.rdb.HGetAll(ctx, chargerKey).Result()
	if err != nil {
		return nil, err
	}

	state := &ChargerState{
		ChargerID: chargerID,
		Vendor:    info["vendor"],
		Model:     info["model"],
		Serial:    info["serial"],
		Firmware:  info["firmware"],
		Online:    info["online"] == "true",
		LastSeen:  info["last_seen"],
		LastBoot:  info["last_boot"],
	}

	connIDs, _ := a.rdb.SMembers(ctx, fmt.Sprintf("charger:%s:connectors", chargerID)).Result()
	sort.Strings(connIDs)

	for _, cidStr := range connIDs {
		connKey := fmt.Sprintf("charger:%s:conn:%s", chargerID, cidStr)
		connInfo, err := a.rdb.HGetAll(ctx, connKey).Result()
		if err != nil {
			continue
		}
		cid := 0
		fmt.Sscanf(cidStr, "%d", &cid)
		cs := ConnectorState{
			ConnectorID:       cid,
			Status:            connInfo["status"],
			ErrorCode:         connInfo["error_code"],
			StatusTimestamp:    connInfo["status_ts"],
			ActiveTransaction: connInfo["active_transaction"] == "true",
			EnergyWh:          connInfo["energy_wh"],
			PowerW:            connInfo["power_w"],
			SoCPercent:        connInfo["soc_percent"],
			MeterTimestamp:    connInfo["meter_ts"],
		}
		state.Connectors = append(state.Connectors, cs)
	}

	return state, nil
}

func writeJSON(w http.ResponseWriter, status int, v interface{}) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	enc := json.NewEncoder(w)
	enc.SetIndent("", "  ")
	enc.Encode(v)
}

func getEnv(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}

const swaggerHTML = `<!DOCTYPE html>
<html lang="en">
<head>
  <meta charset="UTF-8">
  <title>OCPP Charger Telemetry API</title>
  <link rel="stylesheet" href="https://unpkg.com/swagger-ui-dist@5/swagger-ui.css">
  <style>body { margin: 0; } .topbar { display: none; }</style>
</head>
<body>
  <div id="swagger-ui"></div>
  <script src="https://unpkg.com/swagger-ui-dist@5/swagger-ui-bundle.js"></script>
  <script>
    SwaggerUIBundle({
      url: "/swagger/openapi.json",
      dom_id: "#swagger-ui",
      deepLinking: true,
      presets: [SwaggerUIBundle.presets.apis, SwaggerUIBundle.SwaggerUIStandalonePreset],
      layout: "BaseLayout"
    });
  </script>
</body>
</html>`
