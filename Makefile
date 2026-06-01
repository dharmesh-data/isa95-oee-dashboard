.PHONY: dev stop destroy create-topics register-schemas run-simulator stop-simulator run-flink test lint deploy

# ── Local development ──────────────────────────────────────────────────────────

dev:
	cp -n .env.example .env 2>/dev/null || true
	docker-compose up -d zookeeper kafka schema-registry kafka-ui timescaledb grafana
	@echo "Waiting for services to be healthy..."
	@sleep 15
	$(MAKE) create-topics
	$(MAKE) register-schemas
	@echo ""
	@echo "Stack ready."
	@echo "  Grafana:      http://localhost:3000  (admin / admin)"
	@echo "  Kafka UI:     http://localhost:8080"
	@echo "  Schema Reg:   http://localhost:8081"
	@echo "  TimescaleDB:  localhost:5432 (postgres / changeme)"

stop:
	docker-compose stop

destroy:
	docker-compose down -v

create-topics:
	docker exec kafka kafka-topics --bootstrap-server localhost:9092 \
		--create --if-not-exists --topic line-events \
		--partitions 3 --replication-factor 1 \
		--config retention.ms=604800000
	docker exec kafka kafka-topics --bootstrap-server localhost:9092 \
		--create --if-not-exists --topic equipment-status \
		--partitions 3 --replication-factor 1 \
		--config retention.ms=604800000
	docker exec kafka kafka-topics --bootstrap-server localhost:9092 \
		--create --if-not-exists --topic quality-metrics \
		--partitions 3 --replication-factor 1 \
		--config retention.ms=604800000
	@echo "Topics created:"
	docker exec kafka kafka-topics --bootstrap-server localhost:9092 --list

venv:
	python3 -m venv .venv
	.venv/bin/pip install -q requests

register-schemas: venv
	.venv/bin/python scripts/register_schemas.py

run-simulator:
	docker-compose --profile simulator up -d simulator
	@echo "Simulator running. Check Kafka UI: http://localhost:8080"

stop-simulator:
	docker-compose stop simulator

build-flink:
	docker-compose build flink-oee flink-anomaly flink-quality

run-flink:
	docker-compose --profile flink up -d flink-oee flink-anomaly flink-quality
	@echo "Flink jobs running. Check TimescaleDB: make psql"

logs-flink-oee:
	docker-compose logs -f flink-oee

logs-flink-anomaly:
	docker-compose logs -f flink-anomaly

logs-flink-quality:
	docker-compose logs -f flink-quality

# ── Testing ────────────────────────────────────────────────────────────────────

test:
	PYTHONPATH=. pytest flink/tests/ -v --tb=short

lint:
	ruff check .
	black --check .

format:
	black .
	ruff check --fix .

# ── AWS deployment ─────────────────────────────────────────────────────────────

deploy:
	cd terraform && terraform init && terraform apply -var-file=environments/prod.tfvars

plan:
	cd terraform && terraform init && terraform plan -var-file=environments/prod.tfvars

tf-destroy:
	cd terraform && terraform destroy -var-file=environments/prod.tfvars

# ── Utilities ──────────────────────────────────────────────────────────────────

logs-simulator:
	docker-compose logs -f simulator

logs-kafka:
	docker-compose logs -f kafka

check-topics:
	@for topic in line-events equipment-status quality-metrics; do \
		echo "=== $$topic ==="; \
		docker exec kafka kafka-console-consumer \
			--bootstrap-server localhost:9092 \
			--topic $$topic --from-beginning --max-messages 2 2>/dev/null; \
	done

psql:
	docker exec -it timescaledb psql -U postgres -d manufacturing
