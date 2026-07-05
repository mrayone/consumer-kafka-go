package consumer

import (
	"context"
	"crypto/tls"
	"errors"
	"log"
	"time"

	"github.com/segmentio/kafka-go"
	"github.com/segmentio/kafka-go/sasl/plain"

	"github.com/mayconrayone/consumer-kafka-go/internal/config"
	"github.com/mayconrayone/consumer-kafka-go/internal/deserializer"
	"github.com/mayconrayone/consumer-kafka-go/internal/sink"
)

type Consumer struct {
	reader  *kafka.Reader
	decoder deserializer.MessageDeserializer
	sinks   []sink.Sink
	errLog  *log.Logger
}

func New(cfg *config.Config, d deserializer.MessageDeserializer, sinks []sink.Sink, errLog *log.Logger) *Consumer {
	dialer := &kafka.Dialer{
		Timeout:   10 * time.Second,
		DualStack: true,
	}
	if cfg.SecurityProtocol == config.SecuritySASLSSL {
		dialer.TLS = &tls.Config{MinVersion: tls.VersionTLS12}
		dialer.SASLMechanism = plain.Mechanism{
			Username: cfg.Username,
			Password: cfg.Password,
		}
	}
	r := kafka.NewReader(kafka.ReaderConfig{
		Brokers:     []string{cfg.BrokerURL},
		GroupID:     cfg.GroupID,
		Topic:       cfg.TargetTopic,
		StartOffset: kafka.LastOffset,
		Dialer:      dialer,
	})
	return &Consumer{reader: r, decoder: d, sinks: sinks, errLog: errLog}
}

func (c *Consumer) Run(ctx context.Context) error {
	defer c.reader.Close()
	c.errLog.Printf("consuming topic=%s group=%s broker=%s",
		c.reader.Config().Topic, c.reader.Config().GroupID, c.reader.Config().Brokers[0])

	for {
		msg, err := c.reader.ReadMessage(ctx)
		if err != nil {
			if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
				c.errLog.Println("shutting down")
				return nil
			}
			c.errLog.Printf("read error: %v", err)
			continue
		}

		decoded, err := c.decoder.Deserialize(msg.Value)
		if err != nil {
			c.errLog.Printf("deserialize error (offset=%d partition=%d): %v",
				msg.Offset, msg.Partition, err)
			continue
		}
		evt := sink.Event{
			Topic:     msg.Topic,
			Partition: msg.Partition,
			Offset:    msg.Offset,
			Key:       msg.Key,
			Timestamp: msg.Time,
			Decoded:   decoded,
		}
		for _, s := range c.sinks {
			if err := s.Store(ctx, evt); err != nil {
				c.errLog.Printf("sink error (offset=%d partition=%d): %v",
					msg.Offset, msg.Partition, err)
			}
		}
	}
}
