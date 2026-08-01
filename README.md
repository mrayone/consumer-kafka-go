# consumer-kafka-go

A small command-line tool that consumes messages from a Confluent Cloud Kafka topic and sends each decoded record to one or more pluggable sinks (stdout pretty JSON, Postgres event store). It also ships a `seed` subcommand that mass-produces fake events (via [gofakeit](https://github.com/brianvoe/gofakeit)) from a YAML schema, so you can load-test the pipeline end to end.

Supported payload formats, selectable at runtime:

- `json` — plain JSON byte payloads
- `avro` — Confluent wire format (magic byte + schema ID + Avro binary), decoded against a schema fetched from Confluent Schema Registry

## Prerequisites

- Go **1.26.4**
- **Docker + Docker Compose** — for the local stack (Kafka, Schema Registry, Postgres, Kafka UI, Prometheus, Grafana). This is all you need to run and test everything locally; Confluent Cloud is optional.
- A Confluent Cloud cluster with SASL/PLAIN credentials — only if you point the consumer at a hosted cluster instead of the local one.
- For Avro against Confluent Cloud: a Schema Registry endpoint and credentials.

> **Just want to try it?** Jump to [Run & test locally](#run--test-locally) — one stack, a few `make` targets, and a Grafana dashboard.

## Build

```sh
make install        # go mod download + tidy + vendor (resolve all dependencies)
make build          # build the ./consumer-kafka-go binary
```

Or directly with the Go toolchain:

```sh
go mod tidy
go build -o consumer-kafka-go
```

## Configuration

The tool reads a YAML file (see `config.example.yaml`):

```yaml
broker_url: "pkc-xxxxx.region.provider.confluent.cloud:9092"
username: "KAFKA_API_KEY"
password: "KAFKA_API_SECRET"

target_topic: "my-topic"
group_id: "consumer-kafka-go"

schema_registry_url: "https://psrc-xxxxx.region.provider.confluent.cloud"
schema_registry_user: "SR_API_KEY"
schema_registry_pass: "SR_API_SECRET"
schema_subject: "my-topic-value"
```

Schema Registry fields are only required when running with `--format=avro`. `postgres_dsn` is only required when running with `--sink=postgres|both`.

The config also accepts `log_level` (`error|info|debug`, see [Logging & verbosity](#logging--verbosity)), `metrics_addr`, and a `tuning:` block that control logging, the Prometheus endpoint, and consumer throughput/parallelism — see [Throughput tuning](#throughput-tuning) and [Observability](#observability-grafana--prometheus). `config.local.yaml` is a ready-to-use profile for the local Docker stack (PLAINTEXT broker, local Postgres, 4 parallel readers).

## Usage

### Consume

```sh
# JSON-encoded topic, print to stdout (default sink)
./consumer-kafka-go --format=json --config=./config.yaml

# Avro-encoded topic (Confluent wire format)
./consumer-kafka-go --format=avro --config=./config.yaml

# Store decoded events in the Postgres event store (and also print with --sink=both)
./consumer-kafka-go --format=json --config=./config.yaml --sink=postgres
```

### Logging & verbosity

Logging uses [logrus](https://github.com/sirupsen/logrus) and writes to **stderr**
(stdout is reserved for decoded message output). Verbosity is set by `log_level`
in the config file — there is no CLI flag:

| `log_level` | stderr | stdout |
|---|---|---|
| `error` | errors only | — |
| `info` (default) | errors + lifecycle + per-batch consumption sizes | — |
| `debug` | all of the above | decoded messages as JSON |

So `info` keeps the console clean while still reporting throughput; each flush logs:

```
level=info msg="consumed batch" messages=300 decoded=300 bytes=1265421
```

Decoded messages are printed **only at `debug`** (and only when `--sink` includes
`stdout`/`both`). Use `--sink=postgres` for pure ingestion regardless of level.

### Seed fake events

```sh
# Produce 50k fake events from the schema, keyed by event_id
./consumer-kafka-go seed --config=./config.yaml --schema=./seed.schema.yaml \
    --count=50000 --workers=8 --batch=1000 --key-field=event_id
```

The seed schema maps JSON fields to gofakeit generators (see `seed.schema.yaml`):

```yaml
fields:
  event_id: uuid
  event_type: "oneof:user.created,order.placed"
  amount: "price:5,500"
  user:
    fields:
      name: name
      email: email
  tags:
    repeat: 5
    of: word
  description: "lorem:4,8,15"
```

Scalar types: `uuid`, `name`, `firstname`, `lastname`, `email`, `username`, `company`, `city`, `country`, `url`, `ipv4`, `useragent`, `phone`, `word`, `bool`, `date`, `sentence[:words]`, `paragraph[:p,s,w]`, `lorem[:p,s,w]` (bulky text — use it to push payloads past the ~2 KB compression threshold), `int[:min,max]`, `float[:min,max]`, `price[:min,max]`, `oneof:a,b,c` — anything else falls through to gofakeit's template engine (e.g. `hackerphrase`). Nested objects use `fields:`, arrays use `repeat:` + `of:`.

## Postgres event store & compression comparison

With `--sink=postgres|both` every decoded event is written to **two** identical tables that differ only in the TOAST compression method of the `jsonb` payload column:

- `event_store_pglz` — `ALTER COLUMN payload SET COMPRESSION pglz`
- `event_store_lz4` — `ALTER COLUMN payload SET COMPRESSION lz4`

The schema is managed with [goose](https://github.com/pressly/goose) migrations in `migrations/` (goose is wired as a `go tool`, so no separate install is needed):

```sh
make migrate-up                     # apply pending migrations
make migrate-status                 # show migration status
make migrate-down                   # roll back the last migration
make migrate-create NAME=add_thing  # scaffold a new SQL migration
```

Point `PG_DSN` at another database to migrate it (`make migrate-up PG_DSN=postgres://...`). The consumer fails fast with a clear error if the tables are missing.

Note that Postgres only compresses values larger than ~2 KB (TOAST threshold), so make your seeded payloads big enough — the example schema's `description` field exists for exactly that reason.

Local end-to-end run:

```sh
make docker-up                    # full local stack (see "Run & test locally")
make migrate-up                   # create the event_store tables
make run-json-pg                  # consumer with stdout + postgres sinks (leave it running)
make seed SEED_COUNT=50000        # in another terminal
make compare-compression          # pglz vs lz4 size report
```

`make compare-compression` prints per-table totals, table size, and stored payload bytes (`pg_column_size` measures post-compression size):

```
method         | pglz
events         | 50000
total_size     | 312 MB
table_size     | 308 MB
payload_stored | 289 MB
avg_payload    | 6062 bytes
...
```

## Throughput tuning

The consumer fetches with `FetchMessage`, decodes each batch in parallel, flushes
all events to the sinks in **one round-trip per batch**, and commits offsets only
after the batch is persisted (at-least-once; the Postgres sink is idempotent via
`ON CONFLICT DO NOTHING`). Tune it under `tuning:` in the config (defaults shown):

```yaml
metrics_addr: ":2112"     # Prometheus /metrics endpoint (empty disables)
tuning:
  consumers: 4            # parallel readers in the group (cap at partition count)
  min_bytes: 100000       # wait for a real batch per fetch
  max_bytes: 10000000
  queue_capacity: 1000
  max_wait_ms: 500        # caps latency when min_bytes isn't reached
  batch_size: 500         # events per sink flush / offset commit
  batch_timeout_ms: 200   # flush a partial batch after this long
  workers: 4              # concurrent decoders per batch
  lag_poll_ms: 3000
```

### Parallel consumption

`consumers` launches that many `kafka.Reader` goroutines in the same `group_id`;
Kafka's group balancer distributes the topic's partitions across them, so each
reader consumes a disjoint set of partitions concurrently (with `consumers: 4`
on a 4-partition topic, each reader owns one partition). Set it at or below the
partition count — extra readers stay idle. Create the demo topic with 4
partitions via `make topic-create` (defaults to `PARTITIONS=4`); it also grows
an existing topic up to that count.

Each reader runs its own fetch → batch → decode (`workers` goroutines) → flush →
commit pipeline, so peak decode concurrency is `consumers × workers`. To scale
past one process, run more instances in the same group across machines.

## Run & test locally

`make docker-up` brings up the whole stack with Docker Compose:

| Service | Container | URL / port | Purpose |
|---|---|---|---|
| Kafka (KRaft) | `ckg-kafka` | `localhost:9092` | broker |
| Schema Registry | `ckg-schema-registry` | http://localhost:8081 | Avro schemas |
| Postgres 18 | `ckg-postgres` | `localhost:5432` | event store |
| Kafka UI | `ckg-kafka-ui` | http://localhost:8080 | browse topics/messages |
| postgres_exporter | `ckg-postgres-exporter` | `localhost:9187` | DB metrics for Prometheus |
| Prometheus | `ckg-prometheus` | http://localhost:9090 | metrics store |
| Grafana | `ckg-grafana` | http://localhost:3000 | dashboards (anonymous admin) |

The consumer itself runs on the **host** (not in Docker) and exposes Prometheus
metrics on `metrics_addr` (default `:2112`); Prometheus reaches it via
`host.docker.internal`.

End-to-end walkthrough:

```sh
make docker-up                      # start Kafka + Postgres + Prometheus + Grafana + …
make migrate-up                     # create the event_store tables
make topic-create                   # create demo-topic with 4 partitions (PARTITIONS=4)
make seed SEED_COUNT=50000          # produce 50k fake events across the partitions
make run-json-pg                    # consume with 4 parallel readers → Postgres (leave running)
```

Then open **http://localhost:3000** → dashboard **consumer-kafka-go** and watch
lag drain while rows land in Postgres. Stop the consumer with Ctrl-C; tear the
stack down with `make docker-down` (removes volumes). Use `make observability-up`
to start only Prometheus + Grafana + postgres_exporter.

### Verify it's working

```sh
# consumer metrics are being served
curl -s localhost:2112/metrics | grep '^ckg_'

# both Prometheus scrape targets are healthy (consumer-kafka-go + postgres)
curl -s 'localhost:9090/api/v1/targets?state=active' | jq '.data.activeTargets[] | {job:.labels.job, health}'

# the 4 readers each own partitions of the topic (one member per reader)
docker exec ckg-kafka kafka-consumer-groups --bootstrap-server localhost:9092 \
    --describe --group consumer-kafka-go-local --members

# rows are accumulating in the event store
docker exec ckg-postgres psql -U events -d event_store -tAc \
    'SELECT count(*) FROM event_store_lz4;'
```

Unit tests and static checks:

```sh
make test        # go test ./...
make vet         # go vet ./...
make smoke-test  # full stack startup + produce + consume smoke test
```

## Observability (Grafana + Prometheus)

The consumer is instrumented with Prometheus metrics (package `internal/metrics`,
served at `metrics_addr`). Prometheus scrapes the consumer and postgres_exporter
every 5s (`observability/prometheus/prometheus.yml`); Grafana auto-provisions the
datasource and the **consumer-kafka-go** dashboard from `observability/grafana/`.

Exported metrics:

| Metric | Type | Meaning |
|---|---|---|
| `ckg_messages_consumed_total` | counter | messages fetched from Kafka |
| `ckg_events_written_total{sink}` | counter | events persisted, per sink |
| `ckg_decode_errors_total` | counter | deserialization failures |
| `ckg_sink_errors_total{sink}` | counter | failed sink flushes |
| `ckg_batch_flush_duration_seconds{sink}` | histogram | per-batch sink write latency |
| `ckg_batch_size` | histogram | events per flushed batch |
| `ckg_consumer_lag{reader}` | gauge | lag per parallel reader |
| `ckg_pg_event_store_{live_rows,inserts_total,size_bytes}{table}` | from exporter | rows / insert rate / on-disk size per event_store table |

The dashboard panels: consumer lag (per reader), consume vs. write throughput,
sink flush latency (p95), average batch size, decode/sink error rates, and the
database impact (rows, insert rate, and on-disk size for `event_store_pglz` /
`event_store_lz4`). The provisioning files live under `observability/` and are
mounted read-only into the containers, so edits on disk survive restarts.

## Extending the consumer

The consumer fans each batch of decoded events out to every configured `sink.Sink` — plug in anything by implementing:

```go
type Sink interface {
    Store(ctx context.Context, evts []Event) error
    Close() error
}
```

and wiring it in `runConsume` in `main.go`.

For a new consumer group the consumer starts at the **earliest** offset (`StartOffset: FirstOffset`) and then resumes from committed offsets on later runs. Decoded records are written as indented JSON to stdout; lifecycle messages and per-record errors go to stderr, so you can pipe stdout into `jq` or a file without noise:

```sh
./consumer-kafka-go -f avro -c ./config.yaml > messages.jsonl
```

Send `SIGINT` (Ctrl-C) or `SIGTERM` to stop. The reader is closed cleanly and the process exits 0.

## Architecture & Flow

```
  cobra flags ──► load YAML config (+ tuning) ──► deserializer factory
                                          │                     │
                                          │                     └─► /metrics (Prometheus)
                                          ▼
                    tuning.consumers × kafka.Reader (same group_id)
                    SASL/PLAIN + TLS · StartOffset=Earliest
                    Kafka group balancer spreads partitions across readers
                                          │
                    ┌─────────────────────┴─────────────────────┐
                    ▼  per reader:                               ▼
        ┌───────────────────────────┐              (reader N …)
        │ FetchMessage loop         │
        │   └─► batch by size/time  │
        │         └─► decode batch  │  ← workers goroutines
        │              JSON: Unmarshal
        │              Avro: strip 5-byte header ─► schema by ID (cached)
        │                    ─► avro.Unmarshal ─► normalize types
        │         └─► Sink.Store([]Event)  (stdout + postgres, one round-trip)
        │         └─► CommitMessages  (only after persist → at-least-once)
        └───────────────────────────┘
```

### Confluent wire format

Avro payloads on Confluent Cloud are framed as:

```
  byte 0     : magic byte (0x00)
  bytes 1-4  : 4-byte big-endian schema ID
  bytes 5..  : Avro binary-encoded body
```

`internal/deserializer/avro.go` validates the magic byte, extracts the schema ID, fetches the schema via `internal/schemaregistry` (cached in memory by ID), and decodes the body with `github.com/hamba/avro/v2`.

## Adding a new format

1. Implement the `MessageDeserializer` interface in a new file under `internal/deserializer/`:
   ```go
   type MessageDeserializer interface {
       Deserialize(data []byte) (map[string]any, error)
   }
   ```
2. Add a case in `deserializer.New` for the new format string.

The consumer loop, output printer, and CLI plumbing do not need to change.

## Project layout

```
.
├── main.go                       # cobra wiring (consume root + seed subcommand)
├── config.example.yaml           # Confluent Cloud template (+ tuning/metrics)
├── config.local.yaml             # ready-to-use profile for the local Docker stack
├── docker-compose.yml            # Kafka, SR, Postgres, Kafka UI, Prometheus, Grafana, exporter
├── seed.schema.yaml              # example fake-data schema for `seed`
├── migrations/                   # goose SQL migrations for the event store
├── observability/
│   ├── prometheus/prometheus.yml # scrape config (consumer + postgres_exporter)
│   ├── postgres-exporter/        # custom event_store queries
│   └── grafana/                  # provisioned datasource + consumer-kafka-go dashboard
├── internal/
│   ├── config/                   # YAML loader + validation + tuning defaults
│   ├── consumer/                 # N parallel readers, batching pipeline, offset commit
│   ├── deserializer/             # interface + JSON/Avro implementations
│   ├── logging/                  # logrus logger built from log_level
│   ├── metrics/                  # Prometheus collectors + /metrics server
│   ├── schemaregistry/           # Schema Registry client with cache
│   ├── seeder/                   # YAML schema → gofakeit generator → kafka.Writer
│   ├── sink/                     # Sink interface + stdout / postgres event store
│   └── output/                   # MarshalIndent printer
└── README.md
```
