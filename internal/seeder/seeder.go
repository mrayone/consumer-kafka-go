// Package seeder produces fake events to a Kafka topic from a YAML schema.
package seeder

import (
	"context"
	"crypto/tls"
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	"github.com/segmentio/kafka-go"
	"github.com/segmentio/kafka-go/sasl/plain"
	"github.com/sirupsen/logrus"

	"github.com/mayconrayone/consumer-kafka-go/internal/config"
)

type Options struct {
	Count      int
	BatchSize  int
	Workers    int
	Partitions int // used only when the topic has to be created
	KeyField   string
}

type Seeder struct {
	cfg    *config.Config
	schema *Schema
	opts   Options
	log    *logrus.Logger
}

func New(cfg *config.Config, schema *Schema, opts Options, log *logrus.Logger) *Seeder {
	if opts.Count <= 0 {
		opts.Count = 1000
	}
	if opts.BatchSize <= 0 {
		opts.BatchSize = 500
	}
	if opts.Workers <= 0 {
		opts.Workers = 4
	}
	if opts.Partitions <= 0 {
		opts.Partitions = 1
	}
	return &Seeder{cfg: cfg, schema: schema, opts: opts, log: log}
}

func (s *Seeder) transport() *kafka.Transport {
	t := &kafka.Transport{
		Dial: (&net.Dialer{Timeout: 10 * time.Second}).DialContext,
	}
	if s.cfg.SecurityProtocol == config.SecuritySASLSSL {
		t.TLS = &tls.Config{MinVersion: tls.VersionTLS12}
		t.SASL = plain.Mechanism{Username: s.cfg.Username, Password: s.cfg.Password}
	}
	return t
}

func (s *Seeder) ensureTopic(ctx context.Context) error {
	client := &kafka.Client{
		Addr:      kafka.TCP(s.cfg.BrokerURL),
		Transport: s.transport(),
	}
	res, err := client.CreateTopics(ctx, &kafka.CreateTopicsRequest{
		Topics: []kafka.TopicConfig{{
			Topic:             s.cfg.TargetTopic,
			NumPartitions:     s.opts.Partitions,
			ReplicationFactor: 1,
		}},
	})
	if err != nil {
		return fmt.Errorf("create topic %q: %w", s.cfg.TargetTopic, err)
	}
	if topicErr := res.Errors[s.cfg.TargetTopic]; topicErr != nil && !errors.Is(topicErr, kafka.TopicAlreadyExists) {
		return fmt.Errorf("create topic %q: %w", s.cfg.TargetTopic, topicErr)
	}
	return nil
}

// Run produces opts.Count fake events to the target topic using
// opts.Workers concurrent producers writing batches of opts.BatchSize.
func (s *Seeder) Run(ctx context.Context) error {
	if err := s.ensureTopic(ctx); err != nil {
		return err
	}

	writer := &kafka.Writer{
		Addr:         kafka.TCP(s.cfg.BrokerURL),
		Topic:        s.cfg.TargetTopic,
		Balancer:     &kafka.Hash{},
		RequiredAcks: kafka.RequireOne,
		BatchSize:    s.opts.BatchSize,
		BatchTimeout: 100 * time.Millisecond,
		Transport:    s.transport(),
	}
	defer writer.Close()

	s.log.Infof("seeding count=%d workers=%d batch=%d topic=%s broker=%s",
		s.opts.Count, s.opts.Workers, s.opts.BatchSize, s.cfg.TargetTopic, s.cfg.BrokerURL)
	start := time.Now()

	var (
		produced atomic.Int64
		wg       sync.WaitGroup
		errOnce  sync.Once
		runErr   error
	)
	perWorker := s.opts.Count / s.opts.Workers
	remainder := s.opts.Count % s.opts.Workers

	for w := 0; w < s.opts.Workers; w++ {
		n := perWorker
		if w < remainder {
			n++
		}
		if n == 0 {
			continue
		}
		wg.Add(1)
		go func(worker, total int) {
			defer wg.Done()
			gen := NewGenerator(s.schema, uint64(time.Now().UnixNano())+uint64(worker))
			if err := s.produce(ctx, writer, gen, total, &produced); err != nil {
				errOnce.Do(func() { runErr = err })
			}
		}(w, n)
	}
	wg.Wait()

	elapsed := time.Since(start)
	s.log.Infof("seeded %d/%d events in %s (%.0f msg/s)",
		produced.Load(), s.opts.Count, elapsed.Round(time.Millisecond),
		float64(produced.Load())/elapsed.Seconds())
	return runErr
}

func (s *Seeder) produce(ctx context.Context, w *kafka.Writer, gen *Generator, total int, produced *atomic.Int64) error {
	batch := make([]kafka.Message, 0, s.opts.BatchSize)
	flush := func() error {
		if len(batch) == 0 {
			return nil
		}
		if err := w.WriteMessages(ctx, batch...); err != nil {
			return fmt.Errorf("write batch: %w", err)
		}
		n := produced.Add(int64(len(batch)))
		if n%10000 < int64(len(batch)) {
			s.log.Infof("progress: %d events produced", n)
		}
		batch = batch[:0]
		return nil
	}

	for i := 0; i < total; i++ {
		if ctx.Err() != nil {
			return ctx.Err()
		}
		evt, err := gen.Event()
		if err != nil {
			return fmt.Errorf("generate event: %w", err)
		}
		value, err := json.Marshal(evt)
		if err != nil {
			return fmt.Errorf("marshal event: %w", err)
		}
		batch = append(batch, kafka.Message{Key: s.key(evt), Value: value})
		if len(batch) >= s.opts.BatchSize {
			if err := flush(); err != nil {
				return err
			}
		}
	}
	return flush()
}

func (s *Seeder) key(evt map[string]any) []byte {
	if s.opts.KeyField == "" {
		return nil
	}
	switch v := evt[s.opts.KeyField].(type) {
	case nil:
		return nil
	case string:
		return []byte(v)
	case int:
		return []byte(strconv.Itoa(v))
	default:
		b, _ := json.Marshal(v)
		return b
	}
}
