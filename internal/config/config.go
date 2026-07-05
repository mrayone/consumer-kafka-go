package config

import (
	"fmt"
	"os"

	"gopkg.in/yaml.v3"
)

// SecurityProtocol values supported by the consumer.
const (
	SecuritySASLSSL   = "SASL_SSL"  // default — Confluent Cloud
	SecurityPlaintext = "PLAINTEXT" // local docker broker, no auth/TLS
)

type Config struct {
	BrokerURL          string `yaml:"broker_url"`
	SecurityProtocol   string `yaml:"security_protocol"`
	Username           string `yaml:"username"`
	Password           string `yaml:"password"`
	TargetTopic        string `yaml:"target_topic"`
	GroupID            string `yaml:"group_id"`
	SchemaRegistryURL  string `yaml:"schema_registry_url"`
	SchemaRegistryUser string `yaml:"schema_registry_user"`
	SchemaRegistryPass string `yaml:"schema_registry_pass"`
	SchemaSubject      string `yaml:"schema_subject"`
	PostgresDSN        string `yaml:"postgres_dsn"`
}

func Load(path string) (*Config, error) {
	raw, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("read config %q: %w", path, err)
	}
	var c Config
	if err := yaml.Unmarshal(raw, &c); err != nil {
		return nil, fmt.Errorf("parse config %q: %w", path, err)
	}
	return &c, nil
}

func (c *Config) Validate(format string) error {
	missing, err := c.validateCommon()
	if err != nil {
		return err
	}
	if c.GroupID == "" {
		missing = append(missing, "group_id")
	}
	if format == "avro" {
		if c.SchemaRegistryURL == "" {
			missing = append(missing, "schema_registry_url")
		}
	}
	if len(missing) > 0 {
		return fmt.Errorf("missing required config fields: %v", missing)
	}
	return nil
}

// ValidateProducer checks only the fields needed to produce messages
// (no consumer group or schema registry required).
func (c *Config) ValidateProducer() error {
	missing, err := c.validateCommon()
	if err != nil {
		return err
	}
	if len(missing) > 0 {
		return fmt.Errorf("missing required config fields: %v", missing)
	}
	return nil
}

func (c *Config) validateCommon() ([]string, error) {
	var missing []string
	if c.SecurityProtocol == "" {
		c.SecurityProtocol = SecuritySASLSSL
	}
	if c.BrokerURL == "" {
		missing = append(missing, "broker_url")
	}
	if c.SecurityProtocol == SecuritySASLSSL {
		if c.Username == "" {
			missing = append(missing, "username")
		}
		if c.Password == "" {
			missing = append(missing, "password")
		}
	} else if c.SecurityProtocol != SecurityPlaintext {
		return nil, fmt.Errorf("unsupported security_protocol %q (expected SASL_SSL|PLAINTEXT)", c.SecurityProtocol)
	}
	if c.TargetTopic == "" {
		missing = append(missing, "target_topic")
	}
	return missing, nil
}
