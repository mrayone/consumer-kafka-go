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

// Sink receives batches of decoded events. Store is called once per flush
// with all events accumulated since the last flush, letting implementations
// amortize per-message cost (e.g. a single DB round-trip per batch).
// Store may be called concurrently by multiple readers, so implementations
// must be safe for concurrent use.
type Sink interface {
	Store(ctx context.Context, evts []Event) error
	Close() error
}
