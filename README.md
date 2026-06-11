# consumer-kafka-go

A small command-line tool that consumes messages from a Confluent Cloud Kafka topic and prints each record to stdout as pretty-printed JSON. Supports two payload formats selectable at runtime:

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

Schema Registry fields are only required when running with `--format=avro`.

## Usage

```sh
# JSON-encoded topic
./consumer-kafka-go --format=json --config=./config.yaml

# Avro-encoded topic (Confluent wire format)
./consumer-kafka-go --format=avro --config=./config.yaml
```

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
├── main.go                       # cobra wiring + signal handling
├── config.example.yaml
├── internal/
│   ├── config/                   # YAML loader + validation
│   ├── consumer/                 # kafka.Reader + run loop
│   ├── deserializer/             # interface + JSON/Avro implementations
│   ├── schemaregistry/           # Schema Registry client with cache
│   └── output/                   # MarshalIndent printer
└── README.md
```
# consumer-kafka-go
