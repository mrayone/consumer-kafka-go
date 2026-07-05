package seeder

import (
	"fmt"
	"os"
	"strconv"
	"strings"

	"github.com/brianvoe/gofakeit/v7"
	"gopkg.in/yaml.v3"
)

// Schema describes the shape of a seeded event. Each field maps a JSON key
// to a FieldSpec that says how to fake its value.
type Schema struct {
	Fields map[string]*FieldSpec `yaml:"fields"`
}

// FieldSpec is either a scalar generator ("uuid", "int:1,100"), a nested
// object (fields), or an array (repeat + of). In YAML a plain string is
// shorthand for {type: <string>}.
type FieldSpec struct {
	Type   string
	Fields map[string]*FieldSpec
	Repeat int
	Of     *FieldSpec
}

func (s *FieldSpec) UnmarshalYAML(node *yaml.Node) error {
	if node.Kind == yaml.ScalarNode {
		s.Type = node.Value
		return nil
	}
	var raw struct {
		Type   string                `yaml:"type"`
		Fields map[string]*FieldSpec `yaml:"fields"`
		Repeat int                   `yaml:"repeat"`
		Of     *FieldSpec            `yaml:"of"`
	}
	if err := node.Decode(&raw); err != nil {
		return err
	}
	s.Type, s.Fields, s.Repeat, s.Of = raw.Type, raw.Fields, raw.Repeat, raw.Of
	return nil
}

func LoadSchema(path string) (*Schema, error) {
	raw, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("read schema %q: %w", path, err)
	}
	var s Schema
	if err := yaml.Unmarshal(raw, &s); err != nil {
		return nil, fmt.Errorf("parse schema %q: %w", path, err)
	}
	if len(s.Fields) == 0 {
		return nil, fmt.Errorf("schema %q has no fields", path)
	}
	return &s, nil
}

// Generator produces fake events from a Schema. Not safe for concurrent
// use — give each worker its own Generator.
type Generator struct {
	schema *Schema
	faker  *gofakeit.Faker
}

func NewGenerator(schema *Schema, seed uint64) *Generator {
	return &Generator{schema: schema, faker: gofakeit.New(seed)}
}

func (g *Generator) Event() (map[string]any, error) {
	v, err := g.resolveFields(g.schema.Fields)
	if err != nil {
		return nil, err
	}
	return v, nil
}

func (g *Generator) resolveFields(fields map[string]*FieldSpec) (map[string]any, error) {
	out := make(map[string]any, len(fields))
	for name, spec := range fields {
		v, err := g.resolve(spec)
		if err != nil {
			return nil, fmt.Errorf("field %q: %w", name, err)
		}
		out[name] = v
	}
	return out, nil
}

func (g *Generator) resolve(spec *FieldSpec) (any, error) {
	switch {
	case spec == nil:
		return nil, fmt.Errorf("empty field spec")
	case len(spec.Fields) > 0:
		return g.resolveFields(spec.Fields)
	case spec.Of != nil:
		n := spec.Repeat
		if n <= 0 {
			n = 1
		}
		arr := make([]any, n)
		for i := range arr {
			v, err := g.resolve(spec.Of)
			if err != nil {
				return nil, err
			}
			arr[i] = v
		}
		return arr, nil
	default:
		return g.scalar(spec.Type)
	}
}

func (g *Generator) scalar(spec string) (any, error) {
	kind, params, _ := strings.Cut(spec, ":")
	switch kind {
	case "":
		return nil, fmt.Errorf("missing type")
	case "uuid":
		return g.faker.UUID(), nil
	case "name":
		return g.faker.Name(), nil
	case "firstname":
		return g.faker.FirstName(), nil
	case "lastname":
		return g.faker.LastName(), nil
	case "email":
		return g.faker.Email(), nil
	case "username":
		return g.faker.Username(), nil
	case "company":
		return g.faker.Company(), nil
	case "city":
		return g.faker.City(), nil
	case "country":
		return g.faker.Country(), nil
	case "url":
		return g.faker.URL(), nil
	case "ipv4":
		return g.faker.IPv4Address(), nil
	case "useragent":
		return g.faker.UserAgent(), nil
	case "phone":
		return g.faker.Phone(), nil
	case "word":
		return g.faker.Word(), nil
	case "bool":
		return g.faker.Bool(), nil
	case "date":
		return g.faker.Date().Format("2006-01-02T15:04:05Z07:00"), nil
	case "sentence": // sentence[:words]
		words, err := intParams(params, 1, []int{10})
		if err != nil {
			return nil, err
		}
		return g.faker.Sentence(words[0]), nil
	case "paragraph": // paragraph[:paragraphs,sentences,words]
		p, err := intParams(params, 3, []int{2, 5, 12})
		if err != nil {
			return nil, err
		}
		return g.faker.Paragraph(p[0], p[1], p[2], " "), nil
	case "lorem": // lorem[:paragraphs,sentences,words] — bulky text, good for pushing payloads past the ~2KB TOAST compression threshold
		p, err := intParams(params, 3, []int{4, 8, 15})
		if err != nil {
			return nil, err
		}
		return g.faker.LoremIpsumParagraph(p[0], p[1], p[2], " "), nil
	case "int": // int[:min,max]
		p, err := intParams(params, 2, []int{0, 1000000})
		if err != nil {
			return nil, err
		}
		return g.faker.IntRange(p[0], p[1]), nil
	case "float", "price": // float[:min,max]
		min, max := 0.0, 1000.0
		if params != "" {
			parts := strings.SplitN(params, ",", 2)
			if len(parts) != 2 {
				return nil, fmt.Errorf("%s wants min,max, got %q", kind, params)
			}
			var err error
			if min, err = strconv.ParseFloat(strings.TrimSpace(parts[0]), 64); err != nil {
				return nil, fmt.Errorf("%s min: %w", kind, err)
			}
			if max, err = strconv.ParseFloat(strings.TrimSpace(parts[1]), 64); err != nil {
				return nil, fmt.Errorf("%s max: %w", kind, err)
			}
		}
		return g.faker.Price(min, max), nil
	case "oneof": // oneof:a,b,c
		opts := strings.Split(params, ",")
		if params == "" || len(opts) == 0 {
			return nil, fmt.Errorf("oneof wants at least one option")
		}
		return strings.TrimSpace(opts[g.faker.IntRange(0, len(opts)-1)]), nil
	default:
		// Fall through to gofakeit's template engine so any of its
		// functions ("creditcard", "hackerphrase", ...) work as a type.
		v, err := g.faker.Generate("{" + spec + "}")
		if err != nil {
			return nil, fmt.Errorf("unknown type %q: %w", spec, err)
		}
		// gofakeit leaves unknown template functions untouched instead
		// of erroring, so an echoed-back template means a bad type.
		if v == "{"+spec+"}" {
			return nil, fmt.Errorf("unknown type %q", spec)
		}
		return v, nil
	}
}

func intParams(params string, n int, defaults []int) ([]int, error) {
	if params == "" {
		return defaults, nil
	}
	parts := strings.Split(params, ",")
	if len(parts) != n {
		return nil, fmt.Errorf("expected %d params, got %q", n, params)
	}
	out := make([]int, n)
	for i, p := range parts {
		v, err := strconv.Atoi(strings.TrimSpace(p))
		if err != nil {
			return nil, fmt.Errorf("param %q: %w", p, err)
		}
		out[i] = v
	}
	return out, nil
}
