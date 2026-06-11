package deserializer

import (
	"encoding/json"
	"fmt"
)

type JSONDeserializer struct{}

func (JSONDeserializer) Deserialize(data []byte) (map[string]any, error) {
	var out map[string]any
	if err := json.Unmarshal(data, &out); err != nil {
		return nil, fmt.Errorf("json decode: %w", err)
	}
	return out, nil
}
