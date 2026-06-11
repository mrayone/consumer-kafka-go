package deserializer

import (
	"fmt"

	"github.com/mayconrayone/consumer-kafka-go/internal/config"
	"github.com/mayconrayone/consumer-kafka-go/internal/schemaregistry"
)

// MessageDeserializer turns a raw Kafka message payload into a generic map
// that can be re-encoded as JSON for human consumption.
type MessageDeserializer interface {
	Deserialize(data []byte) (map[string]any, error)
}

func New(format string, cfg *config.Config) (MessageDeserializer, error) {
	switch format {
	case "json":
		return &JSONDeserializer{}, nil
	case "avro":
		sr := schemaregistry.NewClient(
			cfg.SchemaRegistryURL,
			cfg.SchemaRegistryUser,
			cfg.SchemaRegistryPass,
		)
		return &AvroDeserializer{sr: sr}, nil
	default:
		return nil, fmt.Errorf("unknown format %q (expected json|avro)", format)
	}
}
