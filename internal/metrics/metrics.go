// Package metrics exposes Prometheus instrumentation for the consumer and
// serves it over an HTTP /metrics endpoint scraped by the local Grafana stack.
package metrics

import (
	"context"
	"errors"
	"net/http"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"github.com/sirupsen/logrus"
)

var (
	// MessagesConsumed counts messages fetched from Kafka.
	MessagesConsumed = promauto.NewCounter(prometheus.CounterOpts{
		Name: "ckg_messages_consumed_total",
		Help: "Total Kafka messages fetched by the consumer.",
	})
	// DecodeErrors counts messages that failed to deserialize.
	DecodeErrors = promauto.NewCounter(prometheus.CounterOpts{
		Name: "ckg_decode_errors_total",
		Help: "Total messages that failed to deserialize.",
	})
	// EventsWritten counts events successfully persisted, per sink.
	EventsWritten = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "ckg_events_written_total",
		Help: "Total events written, labelled by sink.",
	}, []string{"sink"})
	// SinkErrors counts failed sink flushes, per sink.
	SinkErrors = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "ckg_sink_errors_total",
		Help: "Total sink flush errors, labelled by sink.",
	}, []string{"sink"})
	// BatchFlushDuration measures how long a sink flush takes, per sink.
	BatchFlushDuration = promauto.NewHistogramVec(prometheus.HistogramOpts{
		Name:    "ckg_batch_flush_duration_seconds",
		Help:    "Duration of a sink batch flush.",
		Buckets: prometheus.DefBuckets,
	}, []string{"sink"})
	// BatchSize records how many events were in each flushed batch.
	BatchSize = promauto.NewHistogram(prometheus.HistogramOpts{
		Name:    "ckg_batch_size",
		Help:    "Number of events per flushed batch.",
		Buckets: []float64{1, 10, 50, 100, 250, 500, 1000, 2500, 5000},
	})
	// ConsumerLag is the current lag reported by each reader in the group.
	ConsumerLag = promauto.NewGaugeVec(prometheus.GaugeOpts{
		Name: "ckg_consumer_lag",
		Help: "Current consumer lag (messages behind the log end) per reader.",
	}, []string{"reader"})
)

// Serve starts the /metrics HTTP endpoint on addr. It returns immediately;
// the server is shut down when ctx is cancelled. A nil/empty addr is a no-op.
func Serve(ctx context.Context, addr string, log *logrus.Logger) {
	if addr == "" {
		return
	}
	mux := http.NewServeMux()
	mux.Handle("/metrics", promhttp.Handler())
	srv := &http.Server{Addr: addr, Handler: mux}

	go func() {
		log.Infof("metrics listening on %s/metrics", addr)
		if err := srv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			log.WithError(err).Error("metrics server error")
		}
	}()

	go func() {
		<-ctx.Done()
		shutCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_ = srv.Shutdown(shutCtx)
	}()
}
