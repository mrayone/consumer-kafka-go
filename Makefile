.PHONY: help tidy build test vet run-json run-avro \
	docker-build docker-up docker-down docker-logs \
	topic-create produce-json produce-avro register-avro-schema \
	smoke-json smoke-avro smoke-test

BINARY          := consumer-kafka-go
LOCAL_CFG       := config.local.yaml
TOPIC           := demo-topic
SUBJECT         := $(TOPIC)-value
KAFKA_CONTAINER := ckg-kafka
SR_URL          := http://localhost:8081
BROKER_INT      := kafka:29092

help:
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) | sort | awk 'BEGIN {FS = ":.*?## "}; {printf "\033[36m%-22s\033[0m %s\n", $$1, $$2}'

# ---------- Go ----------

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

# ---------- Docker ----------

docker-build: ## Build the consumer container image
	docker build -t $(BINARY):dev .

docker-up: ## Start Confluent Kafka + Schema Registry + Kafka UI
	docker compose up -d
	@echo "waiting for Kafka to be healthy..."
	@until [ "$$(docker inspect -f '{{.State.Health.Status}}' $(KAFKA_CONTAINER) 2>/dev/null)" = "healthy" ]; do sleep 2; done
	@echo "Kafka ready. UI: http://localhost:8080"

docker-down: ## Stop and remove the local stack
	docker compose down -v

docker-logs: ## Tail Kafka logs
	docker compose logs -f kafka

# ---------- Test data helpers ----------

topic-create: ## Create the demo topic
	docker exec $(KAFKA_CONTAINER) kafka-topics \
		--bootstrap-server localhost:9092 \
		--create --topic $(TOPIC) \
		--partitions 1 --replication-factor 1 \
		--if-not-exists

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

# ---------- End-to-end smoke tests ----------

smoke-json: docker-up produce-json build ## Bring stack up, produce JSON, consume for ~5s
	@timeout 5s ./$(BINARY) --format=json --config=$(LOCAL_CFG) || true

smoke-avro: docker-up produce-avro build ## Bring stack up, produce Avro, consume for ~5s
	@timeout 5s ./$(BINARY) --format=avro --config=$(LOCAL_CFG) || true

smoke-test: ## Full smoke startup test (start stack, verify health, produce, consume)
	@bash scripts/smoke-test.sh
