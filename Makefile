.PHONY: help install tidy build test vet run-json run-avro run-json-pg run-avro-pg \
	docker-build docker-up docker-down docker-logs \
	topic-create produce-json produce-avro register-avro-schema \
	seed compare-compression psql \
	migrate-up migrate-down migrate-status migrate-create \
	smoke-json smoke-avro smoke-test

BINARY          := consumer-kafka-go
LOCAL_CFG       := config.local.yaml
TOPIC           := demo-topic
PARTITIONS      := 4
SUBJECT         := $(TOPIC)-value
KAFKA_CONTAINER := ckg-kafka
SR_URL          := http://localhost:8081
BROKER_INT      := kafka:29092

help:
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) | sort | awk 'BEGIN {FS = ":.*?## "}; {printf "\033[36m%-22s\033[0m %s\n", $$1, $$2}'

# ---------- Go ----------

install: ## Resolve, tidy and vendor all dependencies
	go mod download
	go mod tidy
	go mod vendor

tidy: ## go mod tidy
	go mod tidy

build: ## Build the binary
	go build -trimpath -o $(BINARY) .

vet: ## go vet
	go vet ./...

test: ## Run unit tests
	go test ./... -race -count=1

run-json: build ## Run against the local stack in JSON mode
	./$(BINARY) --format=json --config=$(LOCAL_CFG)

run-avro: build ## Run against the local stack in Avro mode
	./$(BINARY) --format=avro --config=$(LOCAL_CFG)

run-json-pg: build ## Consume JSON and store events in the Postgres event store
	./$(BINARY) --format=json --config=$(LOCAL_CFG) --sink=both

run-avro-pg: build ## Consume Avro and store events in the Postgres event store
	./$(BINARY) --format=avro --config=$(LOCAL_CFG) --sink=both

# ---------- Docker ----------

docker-build: ## Build the consumer container image
	docker build -t $(BINARY):dev .

docker-up: ## Start Confluent Kafka + Schema Registry + Kafka UI + observability
	docker compose up -d
	@echo "waiting for Kafka to be healthy..."
	@until [ "$$(docker inspect -f '{{.State.Health.Status}}' $(KAFKA_CONTAINER) 2>/dev/null)" = "healthy" ]; do sleep 2; done
	@echo "Kafka ready. UI: http://localhost:8080"
	@echo "Grafana: http://localhost:3000 (dashboard 'consumer-kafka-go') | Prometheus: http://localhost:9090"

docker-down: ## Stop and remove the local stack
	docker compose down -v

docker-logs: ## Tail Kafka logs
	docker compose logs -f kafka

observability-up: ## Start only Prometheus + Grafana + postgres-exporter
	docker compose up -d postgres-exporter prometheus grafana
	@echo "Grafana: http://localhost:3000 (anonymous admin) | Prometheus: http://localhost:9090"
	@echo "Run the consumer with metrics_addr set (default :2112), e.g. make run-json-pg"

# ---------- Test data helpers ----------

topic-create: ## Create the demo topic (PARTITIONS=4)
	docker exec $(KAFKA_CONTAINER) kafka-topics \
		--bootstrap-server localhost:9092 \
		--create --topic $(TOPIC) \
		--partitions $(PARTITIONS) --replication-factor 1 \
		--if-not-exists
	@# Bump partitions if the topic already existed with fewer (Kafka only grows).
	@docker exec $(KAFKA_CONTAINER) kafka-topics \
		--bootstrap-server localhost:9092 \
		--alter --topic $(TOPIC) --partitions $(PARTITIONS) 2>/dev/null || true
	@docker exec $(KAFKA_CONTAINER) kafka-topics \
		--bootstrap-server localhost:9092 --describe --topic $(TOPIC) | head -1

produce-json: topic-create ## Produce a few JSON records to demo-topic
	@printf '{"id":1,"user":"alice","action":"login"}\n{"id":2,"user":"bob","action":"purchase","amount":42.5}\n{"id":3,"user":"carol","action":"logout"}\n' \
		| docker exec -i $(KAFKA_CONTAINER) kafka-console-producer \
			--bootstrap-server localhost:9092 \
			--topic $(TOPIC)
	@echo "produced 3 JSON records to $(TOPIC)"

register-avro-schema: ## Register the demo Avro schema in Schema Registry
	@curl -sS -X POST -H 'Content-Type: application/vnd.schemaregistry.v1+json' \
		--data '{"schema":"{\"type\":\"record\",\"name\":\"Event\",\"fields\":[{\"name\":\"id\",\"type\":\"long\"},{\"name\":\"user\",\"type\":\"string\"},{\"name\":\"action\",\"type\":\"string\"}]}"}' \
		$(SR_URL)/subjects/$(SUBJECT)/versions | tee /dev/stderr | grep -q '"id"'

produce-avro: topic-create register-avro-schema ## Produce an Avro record (Confluent wire format) to demo-topic
	echo '{"id":1,"user":"alice","action":"login"}' \
		| docker exec -i ckg-schema-registry kafka-avro-console-producer \
			--bootstrap-server $(BROKER_INT) \
			--topic $(TOPIC) \
			--property schema.registry.url=http://schema-registry:8081 \
			--property value.schema.id=1 \
			--property auto.register=false
	@echo "produced 1 Avro record to $(TOPIC)"

# ---------- Seeder & Postgres event store ----------

SEED_SCHEMA := seed.schema.yaml
SEED_COUNT  := 10000
PG_CONTAINER := ckg-postgres
PSQL := docker exec -i $(PG_CONTAINER) psql -U events -d event_store
PG_DSN ?= postgres://events:events@localhost:5432/event_store?sslmode=disable
GOOSE := go tool goose -dir migrations postgres "$(PG_DSN)"

migrate-up: ## Apply all pending goose migrations
	$(GOOSE) up

migrate-down: ## Roll back the last goose migration
	$(GOOSE) down

migrate-status: ## Show goose migration status
	$(GOOSE) status

migrate-create: ## Create a new SQL migration (NAME=add_something)
	@test -n "$(NAME)" || (echo "usage: make migrate-create NAME=add_something" && exit 1)
	go tool goose -dir migrations -s create $(NAME) sql

seed: build ## Produce fake events to the topic (SEED_COUNT=10000 SEED_SCHEMA=seed.schema.yaml)
	./$(BINARY) seed --config=$(LOCAL_CFG) --schema=$(SEED_SCHEMA) \
		--count=$(SEED_COUNT) --key-field=event_id

psql: ## Open a psql shell on the event store
	docker exec -it $(PG_CONTAINER) psql -U events -d event_store

compare-compression: ## Compare pglz vs lz4 on-disk size of the event store tables
	@$(PSQL) -x -c "\
		SELECT 'pglz' AS method, \
		       count(*) AS events, \
		       pg_size_pretty(pg_total_relation_size('event_store_pglz')) AS total_size, \
		       pg_size_pretty(pg_table_size('event_store_pglz')) AS table_size, \
		       pg_size_pretty(sum(pg_column_size(payload))::bigint) AS payload_stored, \
		       pg_size_pretty(avg(pg_column_size(payload))::bigint) AS avg_payload \
		FROM event_store_pglz \
		UNION ALL \
		SELECT 'lz4', count(*), \
		       pg_size_pretty(pg_total_relation_size('event_store_lz4')), \
		       pg_size_pretty(pg_table_size('event_store_lz4')), \
		       pg_size_pretty(sum(pg_column_size(payload))::bigint), \
		       pg_size_pretty(avg(pg_column_size(payload))::bigint) \
		FROM event_store_lz4;"

# ---------- End-to-end smoke tests ----------

smoke-json: docker-up produce-json build ## Bring stack up, produce JSON, consume for ~5s
	@timeout 5s ./$(BINARY) --format=json --config=$(LOCAL_CFG) || true

smoke-avro: docker-up produce-avro build ## Bring stack up, produce Avro, consume for ~5s
	@timeout 5s ./$(BINARY) --format=avro --config=$(LOCAL_CFG) || true

smoke-test: ## Full smoke startup test (start stack, verify health, produce, consume)
	@bash scripts/smoke-test.sh
