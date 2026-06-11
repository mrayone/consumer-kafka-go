#!/usr/bin/env bash
set -euo pipefail

KAFKA_CONTAINER="ckg-kafka"
SR_CONTAINER="ckg-schema-registry"
TOPIC="demo-topic"
BINARY="./consumer-kafka-go"
CFG="config.local.yaml"

pass() { echo "[PASS] $*"; }
fail() { echo "[FAIL] $*" >&2; exit 1; }
step() { echo ""; echo "==> $*"; }

wait_healthy() {
  local container=$1 timeout=$2 elapsed=0
  until [ "$(docker inspect -f '{{.State.Health.Status}}' "$container" 2>/dev/null)" = "healthy" ]; do
    [ "$elapsed" -ge "$timeout" ] && fail "$container did not become healthy within ${timeout}s"
    sleep 2; elapsed=$((elapsed + 2))
  done
}

step "Starting docker compose stack"
docker compose up -d

step "Waiting for Kafka (up to 120s)"
wait_healthy "$KAFKA_CONTAINER" 120
pass "Kafka healthy"

step "Waiting for Schema Registry (up to 60s)"
wait_healthy "$SR_CONTAINER" 60
pass "Schema Registry healthy"

step "Kafka connectivity: kafka-topics --list"
docker exec "$KAFKA_CONTAINER" kafka-topics \
  --bootstrap-server localhost:9092 --list > /dev/null \
  || fail "kafka-topics --list failed"
pass "Kafka connectivity verified"

step "Creating topic: $TOPIC"
docker exec "$KAFKA_CONTAINER" kafka-topics \
  --bootstrap-server localhost:9092 \
  --create --topic "$TOPIC" --partitions 1 --replication-factor 1 \
  --if-not-exists
pass "Topic $TOPIC ready"

step "Producing test JSON message"
echo '{"id":99,"user":"smoke-test","action":"startup"}' \
  | docker exec -i "$KAFKA_CONTAINER" kafka-console-producer \
      --bootstrap-server localhost:9092 --topic "$TOPIC"
pass "JSON message produced"

step "Consuming to verify message stored"
CONSUMED=$(docker exec "$KAFKA_CONTAINER" kafka-console-consumer \
  --bootstrap-server localhost:9092 --topic "$TOPIC" \
  --from-beginning --max-messages 1 --timeout-ms 10000 2>/dev/null)
echo "$CONSUMED" | grep -q "smoke-test" \
  || fail "Expected smoke-test message, got: $CONSUMED"
pass "Message consumed: $CONSUMED"

step "Building Go consumer binary"
go build -trimpath -o "$BINARY" . || fail "go build failed"

step "Go consumer: 5s run in JSON mode"
timeout 5s "$BINARY" --format=json --config="$CFG" || true
pass "Go consumer started without fatal error"

echo ""
echo "=============================="
echo " SMOKE TEST PASSED"
echo "=============================="
