// Package sink defines pluggable destinations for consumed messages.
package sink

import (
	"context"
	"time"
)

// Event is a decoded Kafka message plus its metadata, ready to be stored.
type Event struct {
	Topic     string
	Partition int
	Offset    int64
	Key       []byte
	Timestamp time.Time
	Decoded   map[string]any
}

// Sink receives decoded events. Implementations must be safe for
// sequential use by a single consumer loop.
type Sink interface {
	Store(ctx context.Context, evt Event) error
	Close() error
}
