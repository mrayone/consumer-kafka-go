package main

import (
	"context"
	"fmt"
	"os"
	"os/signal"
	"syscall"

	"github.com/spf13/cobra"

	"github.com/mayconrayone/consumer-kafka-go/internal/config"
	"github.com/mayconrayone/consumer-kafka-go/internal/consumer"
	"github.com/mayconrayone/consumer-kafka-go/internal/deserializer"
	"github.com/mayconrayone/consumer-kafka-go/internal/logging"
	"github.com/mayconrayone/consumer-kafka-go/internal/metrics"
	"github.com/mayconrayone/consumer-kafka-go/internal/output"
	"github.com/mayconrayone/consumer-kafka-go/internal/seeder"
	"github.com/mayconrayone/consumer-kafka-go/internal/sink"
)

func main() {
	var (
		format     string
		configPath string
		sinkName   string
	)

	// Root keeps the original consume behavior so existing invocations
	// (`consumer-kafka-go --format=json --config=...`) still work.
	root := &cobra.Command{
		Use:   "consumer-kafka-go",
		Short: "Kafka consumer with pluggable sinks (stdout, postgres event store) and a fake-data seeder",
		RunE: func(cmd *cobra.Command, _ []string) error {
			return runConsume(cmd.Context(), format, configPath, sinkName)
		},
	}
	root.Flags().StringVarP(&format, "format", "f", "", "message format: json|avro (required)")
	root.Flags().StringVarP(&configPath, "config", "c", "", "path to YAML config file (required)")
	root.Flags().StringVarP(&sinkName, "sink", "s", "stdout", "where to send decoded events: stdout|postgres|both")
	_ = root.MarkFlagRequired("format")
	_ = root.MarkFlagRequired("config")

	root.AddCommand(seedCmd())

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	if err := root.ExecuteContext(ctx); err != nil {
		fmt.Fprintln(os.Stderr, "error:", err)
		os.Exit(1)
	}
}

func seedCmd() *cobra.Command {
	var (
		configPath string
		schemaPath string
		opts       seeder.Options
	)
	cmd := &cobra.Command{
		Use:   "seed",
		Short: "Produce fake events to the target topic from a YAML schema (gofakeit)",
		RunE: func(cmd *cobra.Command, _ []string) error {
			cfg, err := config.Load(configPath)
			if err != nil {
				return err
			}
			if err := cfg.ValidateProducer(); err != nil {
				return err
			}
			cfg.ApplyDefaults()
			schema, err := seeder.LoadSchema(schemaPath)
			if err != nil {
				return err
			}
			logger := logging.New(cfg.LogLevel)
			return seeder.New(cfg, schema, opts, logger).Run(cmd.Context())
		},
	}
	cmd.Flags().StringVarP(&configPath, "config", "c", "", "path to YAML config file (required)")
	cmd.Flags().StringVarP(&schemaPath, "schema", "S", "", "path to YAML seed schema (required)")
	cmd.Flags().IntVarP(&opts.Count, "count", "n", 1000, "number of events to produce")
	cmd.Flags().IntVarP(&opts.BatchSize, "batch", "b", 500, "messages per producer batch")
	cmd.Flags().IntVarP(&opts.Workers, "workers", "w", 4, "concurrent producer workers")
	cmd.Flags().IntVarP(&opts.Partitions, "partitions", "p", 3, "partitions when creating the topic")
	cmd.Flags().StringVarP(&opts.KeyField, "key-field", "k", "", "schema field to use as the message key")
	_ = cmd.MarkFlagRequired("config")
	_ = cmd.MarkFlagRequired("schema")
	return cmd
}

func runConsume(ctx context.Context, format, configPath, sinkName string) error {
	cfg, err := config.Load(configPath)
	if err != nil {
		return err
	}
	if err := cfg.Validate(format); err != nil {
		return err
	}
	cfg.ApplyDefaults()

	logger := logging.New(cfg.LogLevel)
	metrics.Serve(ctx, cfg.MetricsAddr, logger)

	dec, err := deserializer.New(format, cfg)
	if err != nil {
		return err
	}

	// Decoded messages are printed to stdout only at debug level; at info/error
	// the console stays clean (the consumer still logs per-batch sizes at info).
	verbose := config.LogRank(cfg.LogLevel) >= config.LogRank(config.LogDebug)

	var sinks []sink.Sink
	switch sinkName {
	case "stdout", "both":
		if verbose {
			sinks = append(sinks, sink.NewStdout(output.New(os.Stdout)))
		}
	case "postgres":
	default:
		return fmt.Errorf("unsupported sink %q (expected stdout|postgres|both)", sinkName)
	}
	if sinkName == "postgres" || sinkName == "both" {
		if cfg.PostgresDSN == "" {
			return fmt.Errorf("sink %q requires postgres_dsn in the config file", sinkName)
		}
		pg, err := sink.NewPostgres(ctx, cfg.PostgresDSN)
		if err != nil {
			return err
		}
		sinks = append(sinks, pg)
	}
	defer func() {
		for _, s := range sinks {
			_ = s.Close()
		}
	}()

	c := consumer.New(cfg, dec, sinks, logger)
	return c.Run(ctx)
}
