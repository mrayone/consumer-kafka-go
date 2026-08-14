# AGENTS.md

Guidance for agents working in this repository.

## What this is

A Go CLI (`consumer-kafka-go`) that consumes a Kafka topic, decodes each record
(JSON or Confluent-wire Avro), and sends it to pluggable sinks (stdout pretty
JSON, Postgres event store). It also ships a `seed` subcommand that produces
fake events, plus a local Docker stack with Grafana/Prometheus observability.

## Project structure

```text
main.go                       # cobra wiring: consume (root) + seed subcommand
config.example.yaml           # Confluent Cloud template (+ tuning/metrics)
config.local.yaml             # ready-to-use profile for the local Docker stack
docker-compose.yml            # Kafka, Schema Registry, Postgres, Kafka UI, Prometheus, Grafana, postgres_exporter
Makefile                      # all dev/run/test targets (run `make help`)
migrations/                   # goose SQL migrations for the event store
observability/
  prometheus/prometheus.yml   # scrape config (consumer + postgres_exporter)
  postgres-exporter/          # custom event_store queries
  grafana/                    # provisioned datasource + consumer-kafka-go dashboard
internal/
  config/                     # YAML loader, validation, tuning defaults (ApplyDefaults)
  consumer/                   # N parallel readers, batching pipeline, offset commit
  deserializer/               # MessageDeserializer interface + JSON/Avro impls
  logging/                    # logrus logger built from config log_level
  metrics/                    # Prometheus collectors + /metrics HTTP server
  schemaregistry/             # Schema Registry client with in-memory cache
  seeder/                     # YAML schema -> gofakeit -> kafka.Writer
  sink/                       # Sink interface + stdout / postgres event store
  output/                     # MarshalIndent printer
```

## Key commands

- `make install` — go mod download + tidy + vendor
- `make build` — build the `./consumer-kafka-go` binary
- `make test` — run unit tests
- `make vet` — run static checks
- `make docker-up` / `make docker-down` — start / stop the local stack
- `make migrate-up` — create the event store tables
- `make topic-create` — create `demo-topic` with `PARTITIONS=4`
- `make seed SEED_COUNT=50000` — produce fake events
- `make run-json-pg` — consume with 4 parallel readers into Postgres
- `make observability-up` — start only Prometheus + Grafana + postgres_exporter
- `make smoke-test` — full stack startup + produce + consume smoke test

Local endpoints:

- Grafana: [localhost:3000](http://localhost:3000)
- Prometheus: [localhost:9090](http://localhost:9090)
- Kafka UI: [localhost:8080](http://localhost:8080)
- Consumer metrics: `localhost:2112/metrics`

## Architecture notes

- The consumer runs `tuning.consumers` `kafka.Reader`s in one `group_id`; Kafka
  distributes partitions across them for parallel consumption.
- Each reader follows `FetchMessage -> batch by size/time -> parallel decode ->
  Sink.Store([]Event) -> CommitMessages`.
- Offsets are committed only after persistence. Delivery is at-least-once. The
  Postgres sink is idempotent via `ON CONFLICT DO NOTHING`.
- Throughput/parallelism knobs live under `tuning:` in config. Defaults are
  applied in `internal/config` via `ApplyDefaults`.
- Logging uses logrus and writes to stderr. Decoded messages print to stdout
  only at `debug` level.
- Metrics live in `internal/metrics`; update the Grafana dashboard when adding
  important new metrics.

## Working conventions

- Keep [README.md](air-file://uli4p0ou0eb1offgocd1/Users/mayconrayone/Documents/projects/consumer-kafka-go/README.md?type=file&root=%252F) current when changing features, config fields, make targets, metrics, or local workflows.
- When adding a config field, update:
  - `internal/config` defaults and validation
  - [config.example.yaml](air-file://uli4p0ou0eb1offgocd1/Users/mayconrayone/Documents/projects/consumer-kafka-go/config.example.yaml?type=file&root=%252F)
  - [config.local.yaml](air-file://uli4p0ou0eb1offgocd1/Users/mayconrayone/Documents/projects/consumer-kafka-go/config.local.yaml?type=file&root=%252F)
- When adding a `make` target, add it to `.PHONY` and give it a `##` help description.
- `vendor/` is git-ignored; run `make install` after dependency changes.
- Before considering work done, run `make test` and `make vet`.

## Preferred validation flow

Use the smallest relevant validation set:

- Logic changes: `make test`
- Static analysis or API surface changes: `make vet`
- End-to-end/local stack changes: `make smoke-test` or the relevant `make run-*` path
- Migration or Postgres sink changes: `make migrate-up` and verify local insert flow

## Notes for future agents

- Check for overlapping guidance in [CLAUDE.md](air-file://uli4p0ou0eb1offgocd1/Users/mayconrayone/Documents/projects/consumer-kafka-go/CLAUDE.md?type=file&root=%252F) and keep both files aligned if repository conventions change.
- Prefer targeted edits. Avoid broad refactors unless the task requires them.
