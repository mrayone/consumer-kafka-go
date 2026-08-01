# CLAUDE.md

Guidance for working in this repo.

## What this is

A Go CLI (`consumer-kafka-go`) that consumes a Kafka topic, decodes each record
(JSON or Confluent-wire Avro), and fans it out to pluggable sinks (stdout pretty
JSON, Postgres event store). Ships a `seed` subcommand that mass-produces fake
events, and a local Docker stack with Grafana/Prometheus observability.

## Project structure

```
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

- `make install` — go mod download + tidy + vendor (resolve all deps)
- `make build` — build the `./consumer-kafka-go` binary
- `make test` / `make vet` — unit tests / static checks
- `make docker-up` / `make docker-down` — start / tear down the full local stack
- `make migrate-up` — create the event_store tables (goose)
- `make topic-create` — create `demo-topic` with `PARTITIONS=4` (grows an existing topic)
- `make seed SEED_COUNT=50000` — produce fake events
- `make run-json-pg` — consume with 4 parallel readers into Postgres
- `make observability-up` — start only Prometheus + Grafana + postgres_exporter
- `make smoke-test` — full stack startup + produce + consume smoke test

Local endpoints: Grafana http://localhost:3000 (dashboard `consumer-kafka-go`),
Prometheus http://localhost:9090, Kafka UI http://localhost:8080, consumer
metrics `localhost:2112/metrics`.

## Architecture notes

- The consumer runs `tuning.consumers` `kafka.Reader`s in one `group_id`; the group
  balancer distributes partitions across them (true parallel consume).
- Each reader: `FetchMessage` -> batch by size/time -> parallel decode
  (`tuning.workers`) -> `Sink.Store([]Event)` (one round-trip) -> `CommitMessages`
  **only after** persist (at-least-once; Postgres sink is idempotent via
  `ON CONFLICT DO NOTHING`).
- Throughput/parallelism knobs live under `tuning:` in the config; defaults are
  applied in `internal/config` `ApplyDefaults`.
- Logging is logrus (`internal/logging`), to stderr, level from config `log_level`
  (`error|info|debug`). Log errors at `.Error`, lifecycle/throughput at `.Info`.
  Decoded messages print to stdout only at `debug`. There is no `--silent`/CLI flag.
- Metrics are defined in `internal/metrics`; add new ones there and, if useful,
  a panel in `observability/grafana/dashboards/consumer.json`.

## Conventions / reminders

- **Keep the README current.** When adding a feature, config field, `make` target,
  metric, or changing how to run/test things, update `README.md` in the same change
  (relevant sections: Run & test locally, Observability, Throughput tuning,
  Configuration, Project layout).
- When adding a config field, also update `ApplyDefaults`, `config.example.yaml`,
  and `config.local.yaml`.
- When adding a `make` target, add it to `.PHONY` and give it a `## description`
  so it shows up in `make help`.
- `vendor/` is git-ignored; run `make install` after changing dependencies.
- Run `make test` and `make vet` before considering a change done.
