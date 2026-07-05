package seeder

import (
	"encoding/json"
	"testing"

	"gopkg.in/yaml.v3"
)

const testSchema = `
fields:
  event_id: uuid
  event_type: "oneof:user.created,order.placed"
  amount: "price:5,500"
  active: bool
  quantity: "int:1,10"
  user:
    fields:
      name: name
      email: email
  tags:
    repeat: 3
    of: word
  description: "paragraph:2,3,5"
`

func loadTestSchema(t *testing.T) *Schema {
	t.Helper()
	var s Schema
	if err := yaml.Unmarshal([]byte(testSchema), &s); err != nil {
		t.Fatalf("parse schema: %v", err)
	}
	return &s
}

func TestGeneratorEvent(t *testing.T) {
	gen := NewGenerator(loadTestSchema(t), 42)

	evt, err := gen.Event()
	if err != nil {
		t.Fatalf("Event() error: %v", err)
	}

	if _, err := json.Marshal(evt); err != nil {
		t.Fatalf("event not JSON-serializable: %v", err)
	}

	if s, ok := evt["event_id"].(string); !ok || len(s) != 36 {
		t.Errorf("event_id = %v, want uuid string", evt["event_id"])
	}
	et := evt["event_type"].(string)
	if et != "user.created" && et != "order.placed" {
		t.Errorf("event_type = %q, want one of the declared options", et)
	}
	if amt, ok := evt["amount"].(float64); !ok || amt < 5 || amt > 500 {
		t.Errorf("amount = %v, want float in [5,500]", evt["amount"])
	}
	if _, ok := evt["active"].(bool); !ok {
		t.Errorf("active = %v, want bool", evt["active"])
	}
	if q, ok := evt["quantity"].(int); !ok || q < 1 || q > 10 {
		t.Errorf("quantity = %v, want int in [1,10]", evt["quantity"])
	}
	user, ok := evt["user"].(map[string]any)
	if !ok {
		t.Fatalf("user = %v, want nested object", evt["user"])
	}
	if _, ok := user["email"].(string); !ok {
		t.Errorf("user.email = %v, want string", user["email"])
	}
	tags, ok := evt["tags"].([]any)
	if !ok || len(tags) != 3 {
		t.Errorf("tags = %v, want array of 3", evt["tags"])
	}
	if d, ok := evt["description"].(string); !ok || d == "" {
		t.Errorf("description = %v, want non-empty string", evt["description"])
	}
}

func TestGeneratorUnknownType(t *testing.T) {
	var s Schema
	if err := yaml.Unmarshal([]byte("fields:\n  x: definitely-not-a-faker-fn"), &s); err != nil {
		t.Fatal(err)
	}
	if _, err := NewGenerator(&s, 1).Event(); err == nil {
		t.Fatal("expected error for unknown type")
	}
}

func TestGeneratorGofakeitFallback(t *testing.T) {
	var s Schema
	if err := yaml.Unmarshal([]byte("fields:\n  phrase: hackerphrase"), &s); err != nil {
		t.Fatal(err)
	}
	evt, err := NewGenerator(&s, 1).Event()
	if err != nil {
		t.Fatalf("Event() error: %v", err)
	}
	if p, ok := evt["phrase"].(string); !ok || p == "" {
		t.Errorf("phrase = %v, want non-empty string from gofakeit template", evt["phrase"])
	}
}
