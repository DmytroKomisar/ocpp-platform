.PHONY: build up down logs test clean status

build:
	docker compose build

up:
	docker compose up -d
	@echo ""
	@echo "Services starting..."
	@echo "  WSGW:     http://localhost:8080/healthz"
	@echo "  API:      http://localhost:8081/chargers"
	@echo ""
	@echo "Wait ~15s for chargers to connect, then:"
	@echo "  curl http://localhost:8081/chargers"
	@echo "  curl http://localhost:8081/chargers/SPI-00001/state"

down:
	docker compose down -v

logs:
	docker compose logs -f

logs-wsgw:
	docker compose logs -f wsgw

logs-sim:
	docker compose logs -f cp-simulator

logs-proc:
	docker compose logs -f state-processor

status:
	@echo "=== WSGW Health ==="
	@curl -s http://localhost:8080/healthz 2>/dev/null | python3 -m json.tool || echo "not running"
	@echo "\n=== WSGW Metrics ==="
	@curl -s http://localhost:8080/metrics 2>/dev/null || echo "not running"
	@echo "\n=== Chargers ==="
	@curl -s http://localhost:8081/chargers 2>/dev/null | python3 -m json.tool || echo "not running"

test: up
	@echo "Waiting 20s for chargers to produce data..."
	@sleep 20
	@echo ""
	@echo "=== WSGW Health ==="
	@curl -sf http://localhost:8080/healthz | python3 -m json.tool
	@echo ""
	@echo "=== Charger List ==="
	@curl -sf http://localhost:8081/chargers | python3 -m json.tool
	@echo ""
	@echo "=== Single Charger State ==="
	@curl -sf http://localhost:8081/chargers/SPI-00001/state | python3 -m json.tool
	@echo ""
	@echo "TEST PASSED: All endpoints responding"

clean: down
	docker image prune -f
