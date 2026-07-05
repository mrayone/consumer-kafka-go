# consumer-kafka-go

A small command-line tool that consumes messages from a Confluent Cloud Kafka topic and sends each decoded record to one or more pluggable sinks (stdout pretty JSON, Postgres event store). It also ships a `seed` subcommand that mass-produces fake events (via [gofakeit](https://github.com/brianvoe/gofakeit)) from a YAML schema, so you can load-test the pipeline end to end.

Supported payload formats, selectable at runtime:

- `json` — plain JSON byte payloads
- `avro` — Confluent wire format (magic byte + schema ID + Avro binary), decoded against a schema fetched from Confluent Schema Registry

## Prerequisites

- Go **1.26.4**
- A Confluent Cloud cluster with SASL/PLAIN credentials
- For Avro: a Confluent Schema Registry endpoint and credentials

## Build

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

The schema is created automatically on startup. Note that Postgres only compresses values larger than ~2 KB (TOAST threshold), so make your seeded payloads big enough — the example schema's `description` paragraph exists for exactly that reason.

Local end-to-end run:

```sh
make docker-up                    # Kafka + Schema Registry + Postgres + Kafka UI
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

## Extending the consumer

The consumer loop fans each decoded event out to every configured `sink.Sink` — plug in anything by implementing:

```go
type Sink interface {
    Store(ctx context.Context, evt Event) error
    Close() error
}
```

and wiring it in `runConsume` in `main.go`.

The consumer starts at the **latest** offset for the configured group, so only newly produced messages are printed. Decoded records are written as indented JSON to stdout; lifecycle messages and per-record errors go to stderr, so you can pipe stdout into `jq` or a file without noise:

```sh
./consumer-kafka-go -f avro -c ./config.yaml > messages.jsonl
```

Send `SIGINT` (Ctrl-C) or `SIGTERM` to stop. The reader is closed cleanly and the process exits 0.

## Architecture & Flow

```
  cobra flags ──► load YAML config ──► deserializer factory
                                          │
                                          ▼
                              ┌───────────────────────┐
                              │  kafka.Reader         │
                              │  SASL/PLAIN + TLS     │
                              │  StartOffset=Latest   │
                              └───────────┬───────────┘
                                          │ ReadMessage loop
                                          ▼
                  ┌───────────────────────────────────────┐
                  │ JSON: json.Unmarshal                  │
                  │ Avro: strip 5-byte header             │
                  │       ─► fetch schema by ID (cached)  │
                  │       ─► avro.Unmarshal               │
                  │       ─► normalize bytes/time types   │
                  └───────────────────┬───────────────────┘
                                      ▼
                              map[string]any
                                      │
                                      ▼
                       json.MarshalIndent ─► stdout
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
├── config.example.yaml
├── seed.schema.yaml              # example fake-data schema for `seed`
├── internal/
│   ├── config/                   # YAML loader + validation
│   ├── consumer/                 # kafka.Reader + run loop fanning out to sinks
│   ├── deserializer/             # interface + JSON/Avro implementations
│   ├── schemaregistry/           # Schema Registry client with cache
│   ├── seeder/                   # YAML schema → gofakeit generator → kafka.Writer
│   ├── sink/                     # Sink interface + stdout / postgres event store
│   └── output/                   # MarshalIndent printer
└── README.md
```
