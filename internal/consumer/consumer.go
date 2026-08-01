package consumer

import (
	"context"
	"crypto/tls"
	"errors"
	"log"
	"strconv"
	"sync"
	"time"

	"github.com/segmentio/kafka-go"
	"github.com/segmentio/kafka-go/sasl/plain"
	"golang.org/x/sync/errgroup"

	"github.com/mayconrayone/consumer-kafka-go/internal/config"
	"github.com/mayconrayone/consumer-kafka-go/internal/deserializer"
	"github.com/mayconrayone/consumer-kafka-go/internal/metrics"
	"github.com/mayconrayone/consumer-kafka-go/internal/sink"
)

// namedSink pairs a sink with a stable label for metrics.
type namedSink struct {
	name string
	sink sink.Sink
}

type Consumer struct {
	cfg     *config.Config
	decoder deserializer.MessageDeserializer
	sinks   []namedSink
	errLog  *log.Logger
	tuning  config.TuningConfig
}

func New(cfg *config.Config, d deserializer.MessageDeserializer, sinks []sink.Sink, errLog *log.Logger) *Consumer {
	named := make([]namedSink, 0, len(sinks))
	for _, s := range sinks {
		named = append(named, namedSink{name: sinkName(s), sink: s})
	}
	return &Consumer{cfg: cfg, decoder: d, sinks: named, errLog: errLog, tuning: cfg.Tuning}
}

func sinkName(s sink.Sink) string {
	switch s.(type) {
	case *sink.Postgres:
		return "postgres"
	case *sink.Stdout:
		return "stdout"
	default:
		return "unknown"
	}
}

// newReader builds one group reader. Running several with the same GroupID lets
// the group balancer distribute the topic's partitions across them, giving true
// parallel consumption within a single process.
func (c *Consumer) newReader() *kafka.Reader {
	dialer := &kafka.Dialer{
		Timeout:   10 * time.Second,
		DualStack: true,
	}
	if c.cfg.SecurityProtocol == config.SecuritySASLSSL {
		dialer.TLS = &tls.Config{MinVersion: tls.VersionTLS12}
		dialer.SASLMechanism = plain.Mechanism{
			Username: c.cfg.Username,
			Password: c.cfg.Password,
		}
	}
	t := c.tuning
	return kafka.NewReader(kafka.ReaderConfig{
		Brokers:       []string{c.cfg.BrokerURL},
		GroupID:       c.cfg.GroupID,
		Topic:         c.cfg.TargetTopic,
		StartOffset:   kafka.FirstOffset,
		Dialer:        dialer,
		MinBytes:      t.MinBytes,
		MaxBytes:      t.MaxBytes,
		QueueCapacity: t.QueueCapacity,
		MaxWait:       time.Duration(t.MaxWaitMs) * time.Millisecond,
		// CommitInterval is intentionally left at 0: we commit explicitly
		// after each batch is persisted (at-least-once semantics).
	})
}

// Run launches tuning.Consumers reader goroutines, each consuming its share of
// the topic's partitions concurrently. It returns when ctx is cancelled or a
// reader hits a fatal error.
func (c *Consumer) Run(ctx context.Context) error {
	n := c.tuning.Consumers
	if n < 1 {
		n = 1
	}
	c.errLog.Printf("starting %d concurrent readers: topic=%s group=%s broker=%s batch_size=%d workers=%d",
		n, c.cfg.TargetTopic, c.cfg.GroupID, c.cfg.BrokerURL, c.tuning.BatchSize, c.tuning.Workers)

	g, ctx := errgroup.WithContext(ctx)
	for i := 0; i < n; i++ {
		id := i
		g.Go(func() error { return c.runReader(ctx, id) })
	}
	return g.Wait()
}

// runReader owns a single kafka.Reader and its batching pipeline.
func (c *Consumer) runReader(ctx context.Context, id int) error {
	reader := c.newReader()
	defer reader.Close()
	readerID := strconv.Itoa(id)

	go c.pollLag(ctx, reader, readerID)

	// Decouple fetching from batching so a partial batch can flush on a timer
	// without blocking in FetchMessage.
	msgCh := make(chan kafka.Message, c.tuning.QueueCapacity)
	fetchErr := make(chan error, 1)
	go c.fetchLoop(ctx, reader, msgCh, fetchErr)

	batchTimeout := time.Duration(c.tuning.BatchTimeoutMs) * time.Millisecond
	batch := make([]kafka.Message, 0, c.tuning.BatchSize)
	timer := time.NewTimer(batchTimeout)
	timer.Stop()

	stopTimer := func() {
		if !timer.Stop() {
			select {
			case <-timer.C:
			default:
			}
		}
	}

	for {
		select {
		case m := <-msgCh:
			if len(batch) == 0 {
				timer.Reset(batchTimeout)
			}
			batch = append(batch, m)
			if len(batch) >= c.tuning.BatchSize {
				stopTimer()
				if err := c.flush(ctx, reader, batch); err != nil {
					return err
				}
				batch = batch[:0]
			}

		case <-timer.C:
			if len(batch) > 0 {
				if err := c.flush(ctx, reader, batch); err != nil {
					return err
				}
				batch = batch[:0]
			}

		case err := <-fetchErr:
			// Drain whatever we've buffered before deciding what to do.
			if len(batch) > 0 {
				if ferr := c.flush(ctx, reader, batch); ferr != nil {
					return ferr
				}
				batch = batch[:0]
			}
			if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
				c.errLog.Printf("reader %s shutting down", readerID)
				return nil
			}
			return err

		case <-ctx.Done():
			c.errLog.Printf("reader %s shutting down", readerID)
			return nil
		}
	}
}

func (c *Consumer) fetchLoop(ctx context.Context, reader *kafka.Reader, out chan<- kafka.Message, fetchErr chan<- error) {
	for {
		m, err := reader.FetchMessage(ctx)
		if err != nil {
			fetchErr <- err
			return
		}
		metrics.MessagesConsumed.Inc()
		select {
		case out <- m:
		case <-ctx.Done():
			return
		}
	}
}

// flush decodes the batch, writes it to every sink, and commits the offsets
// only if all sinks succeeded. On sink failure the batch is left uncommitted so
// it is redelivered later (the Postgres sink is idempotent via ON CONFLICT).
func (c *Consumer) flush(ctx context.Context, reader *kafka.Reader, msgs []kafka.Message) error {
	events := c.decodeBatch(msgs)
	if len(events) > 0 {
		metrics.BatchSize.Observe(float64(len(events)))
	}

	allOK := true
	for _, ns := range c.sinks {
		if len(events) == 0 {
			break
		}
		start := time.Now()
		err := ns.sink.Store(ctx, events)
		metrics.BatchFlushDuration.WithLabelValues(ns.name).Observe(time.Since(start).Seconds())
		if err != nil {
			allOK = false
			metrics.SinkErrors.WithLabelValues(ns.name).Inc()
			c.errLog.Printf("sink %s error (%d events): %v", ns.name, len(events), err)
			continue
		}
		metrics.EventsWritten.WithLabelValues(ns.name).Add(float64(len(events)))
	}

	// Only advance offsets when the batch is safely persisted everywhere.
	if allOK {
		if err := reader.CommitMessages(ctx, msgs...); err != nil {
			if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
				return nil
			}
			return err
		}
	}
	return nil
}

// decodeBatch deserializes msgs concurrently (bounded by Workers) and returns
// only the successfully decoded events. Decode failures are logged and skipped.
func (c *Consumer) decodeBatch(msgs []kafka.Message) []sink.Event {
	decoded := make([]sink.Event, len(msgs))
	ok := make([]bool, len(msgs))

	sem := make(chan struct{}, c.tuning.Workers)
	var wg sync.WaitGroup
	for i := range msgs {
		wg.Add(1)
		sem <- struct{}{}
		go func(i int) {
			defer wg.Done()
			defer func() { <-sem }()
			m := msgs[i]
			d, err := c.decoder.Deserialize(m.Value)
			if err != nil {
				metrics.DecodeErrors.Inc()
				c.errLog.Printf("deserialize error (offset=%d partition=%d): %v", m.Offset, m.Partition, err)
				return
			}
			decoded[i] = sink.Event{
				Topic:     m.Topic,
				Partition: m.Partition,
				Offset:    m.Offset,
				Key:       m.Key,
				Timestamp: m.Time,
				Decoded:   d,
			}
			ok[i] = true
		}(i)
	}
	wg.Wait()

	out := decoded[:0]
	for i := range decoded {
		if ok[i] {
			out = append(out, decoded[i])
		}
	}
	return out
}

// pollLag publishes a reader's current lag to Prometheus on an interval.
func (c *Consumer) pollLag(ctx context.Context, reader *kafka.Reader, readerID string) {
	interval := time.Duration(c.tuning.LagPollMs) * time.Millisecond
	if interval <= 0 {
		return
	}
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			metrics.ConsumerLag.WithLabelValues(readerID).Set(float64(reader.Stats().Lag))
		}
	}
}
