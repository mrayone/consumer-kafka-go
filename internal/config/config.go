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

// Log levels, ordered by increasing verbosity. Errors always print.
//   - LogError: only errors.
//   - LogInfo:  + lifecycle and per-batch consumption sizes (default).
//   - LogDebug: + decoded messages printed as JSON to stdout.
const (
	LogError = "error"
	LogInfo  = "info"
	LogDebug = "debug"
)

// LogRank maps a log level to a numeric severity (higher = more verbose);
// unknown values fall back to info.
func LogRank(level string) int {
	switch level {
	case LogError:
		return 0
	case LogDebug:
		return 2
	default:
		return 1 // info
	}
}

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

	// LogLevel controls stderr verbosity and whether decoded messages are
	// printed to stdout: error | info | debug. Default "info".
	LogLevel string `yaml:"log_level"`

	// MetricsAddr is the listen address for the Prometheus /metrics endpoint.
	// Empty disables metrics. Default ":2112".
	MetricsAddr string `yaml:"metrics_addr"`

	// Tuning controls consumer fetch/batch throughput. All fields have
	// sensible defaults applied by ApplyDefaults.
	Tuning TuningConfig `yaml:"tuning"`
}

// TuningConfig groups the knobs that trade latency for throughput.
type TuningConfig struct {
	Consumers      int `yaml:"consumers"`        // parallel readers in the group (cap at partition count)
	MinBytes       int `yaml:"min_bytes"`        // min bytes per fetch (wait for a real batch)
	MaxBytes       int `yaml:"max_bytes"`        // max bytes per fetch
	QueueCapacity  int `yaml:"queue_capacity"`   // reader prefetch buffer
	MaxWaitMs      int `yaml:"max_wait_ms"`      // max time to wait for MinBytes
	BatchSize      int `yaml:"batch_size"`       // events buffered before a sink flush
	BatchTimeoutMs int `yaml:"batch_timeout_ms"` // max time to wait before flushing a partial batch
	Workers        int `yaml:"workers"`          // concurrent decoders per batch
	LagPollMs      int `yaml:"lag_poll_ms"`      // interval for polling reader lag into metrics
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

// ApplyDefaults fills unset tuning/metrics fields with production-sane values.
// Call after Load and before building the consumer.
func (c *Config) ApplyDefaults() {
	if c.LogLevel == "" {
		c.LogLevel = LogInfo
	}
	if c.MetricsAddr == "" {
		c.MetricsAddr = ":2112"
	}
	t := &c.Tuning
	if t.Consumers == 0 {
		t.Consumers = 1
	}
	if t.MinBytes == 0 {
		t.MinBytes = 100_000 // 100 KB
	}
	if t.MaxBytes == 0 {
		t.MaxBytes = 10_000_000 // 10 MB
	}
	if t.QueueCapacity == 0 {
		t.QueueCapacity = 1000
	}
	if t.MaxWaitMs == 0 {
		t.MaxWaitMs = 500
	}
	if t.BatchSize == 0 {
		t.BatchSize = 500
	}
	if t.BatchTimeoutMs == 0 {
		t.BatchTimeoutMs = 200
	}
	if t.Workers == 0 {
		t.Workers = 4
	}
	if t.LagPollMs == 0 {
		t.LagPollMs = 3000
	}
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
	switch c.LogLevel {
	case "", LogError, LogInfo, LogDebug:
	default:
		return fmt.Errorf("unsupported log_level %q (expected error|info|debug)", c.LogLevel)
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
