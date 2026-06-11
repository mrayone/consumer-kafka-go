package deserializer

import (
	"encoding/binary"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/mayconrayone/consumer-kafka-go/internal/schemaregistry"

	"github.com/hamba/avro/v2"
)

func TestAvroDeserializer_WireFormatTooShort(t *testing.T) {
	d := &AvroDeserializer{}
	if _, err := d.Deserialize([]byte{0x00, 0x01}); err == nil {
		t.Fatal("expected error for too-short payload")
	}
}

func TestAvroDeserializer_BadMagicByte(t *testing.T) {
	d := &AvroDeserializer{}
	if _, err := d.Deserialize([]byte{0x01, 0, 0, 0, 1, 0xab}); err == nil ||
		!strings.Contains(err.Error(), "magic byte") {
		t.Fatalf("expected magic byte error, got %v", err)
	}
}

func TestAvroDeserializer_DecodesWithSchemaFromRegistry(t *testing.T) {
	schemaJSON := `{"type":"record","name":"Event","fields":[{"name":"id","type":"long"},{"name":"user","type":"string"}]}`

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if !strings.HasPrefix(r.URL.Path, "/schemas/ids/") {
			http.NotFound(w, r)
			return
		}
		w.Header().Set("Content-Type", "application/vnd.schemaregistry.v1+json")
		_, _ = w.Write([]byte(`{"schema":` + mustJSONString(schemaJSON) + `}`))
	}))
	defer srv.Close()

	schema, err := avro.Parse(schemaJSON)
	if err != nil {
		t.Fatal(err)
	}
	body, err := avro.Marshal(schema, map[string]any{"id": int64(7), "user": "alice"})
	if err != nil {
		t.Fatal(err)
	}

	payload := make([]byte, 5+len(body))
	payload[0] = 0x00
	binary.BigEndian.PutUint32(payload[1:5], 42)
	copy(payload[5:], body)

	d := &AvroDeserializer{sr: schemaregistry.NewClient(srv.URL, "", "")}
	out, err := d.Deserialize(payload)
	if err != nil {
		t.Fatalf("Deserialize: %v", err)
	}
	if out["user"] != "alice" || out["id"].(int64) != 7 {
		t.Fatalf("unexpected output: %#v", out)
	}
}

// mustJSONString returns a JSON-quoted string literal for embedding inside another JSON document.
func mustJSONString(s string) string {
	var b strings.Builder
	b.WriteByte('"')
	for _, r := range s {
		switch r {
		case '"', '\\':
			b.WriteByte('\\')
			b.WriteRune(r)
		case '\n':
			b.WriteString(`\n`)
		default:
			b.WriteRune(r)
		}
	}
	b.WriteByte('"')
	return b.String()
}
