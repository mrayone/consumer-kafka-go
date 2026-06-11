package deserializer

import (
	"encoding/base64"
	"encoding/binary"
	"fmt"
	"time"

	"github.com/hamba/avro/v2"
	"github.com/mayconrayone/consumer-kafka-go/internal/schemaregistry"
)

const confluentMagicByte = 0x00

type AvroDeserializer struct {
	sr *schemaregistry.Client
}

func (a *AvroDeserializer) Deserialize(data []byte) (map[string]any, error) {
	if len(data) < 5 {
		return nil, fmt.Errorf("avro payload too short (%d bytes)", len(data))
	}
	if data[0] != confluentMagicByte {
		return nil, fmt.Errorf("invalid magic byte 0x%02x (expected 0x00)", data[0])
	}
	schemaID := binary.BigEndian.Uint32(data[1:5])
	body := data[5:]

	schema, err := a.sr.GetByID(schemaID)
	if err != nil {
		return nil, fmt.Errorf("fetch schema id=%d: %w", schemaID, err)
	}

	var out map[string]any
	if err := avro.Unmarshal(schema, body, &out); err != nil {
		return nil, fmt.Errorf("avro decode (schema id=%d): %w", schemaID, err)
	}

	return normalize(out).(map[string]any), nil
}

// normalize walks the decoded value tree and converts types that don't
// round-trip cleanly through encoding/json (raw bytes, time.Time) into
// human-readable representations.
func normalize(v any) any {
	switch x := v.(type) {
	case map[string]any:
		for k, vv := range x {
			x[k] = normalize(vv)
		}
		return x
	case []any:
		for i, vv := range x {
			x[i] = normalize(vv)
		}
		return x
	case []byte:
		return base64.StdEncoding.EncodeToString(x)
	case time.Time:
		return x.UTC().Format(time.RFC3339Nano)
	case time.Duration:
		return x.String()
	default:
		return v
	}
}
