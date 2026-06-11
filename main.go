package main

import (
	"context"
	"fmt"
	"log"
	"os"
	"os/signal"
	"syscall"

	"github.com/spf13/cobra"

	"github.com/mayconrayone/consumer-kafka-go/internal/config"
	"github.com/mayconrayone/consumer-kafka-go/internal/consumer"
	"github.com/mayconrayone/consumer-kafka-go/internal/deserializer"
	"github.com/mayconrayone/consumer-kafka-go/internal/output"
)

func main() {
	var (
		format     string
		configPath string
	)

	cmd := &cobra.Command{
		Use:   "consumer-kafka-go",
		Short: "Confluent Kafka consumer that prints messages as pretty JSON",
		RunE: func(cmd *cobra.Command, _ []string) error {
			return run(cmd.Context(), format, configPath)
		},
	}

	cmd.Flags().StringVarP(&format, "format", "f", "", "message format: json|avro (required)")
	cmd.Flags().StringVarP(&configPath, "config", "c", "", "path to YAML config file (required)")
	_ = cmd.MarkFlagRequired("format")
	_ = cmd.MarkFlagRequired("config")

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	if err := cmd.ExecuteContext(ctx); err != nil {
		fmt.Fprintln(os.Stderr, "error:", err)
		os.Exit(1)
	}
}

func run(ctx context.Context, format, configPath string) error {
	cfg, err := config.Load(configPath)
	if err != nil {
		return err
	}
	if err := cfg.Validate(format); err != nil {
		return err
	}

	dec, err := deserializer.New(format, cfg)
	if err != nil {
		return err
	}

	errLog := log.New(os.Stderr, "[consumer-kafka-go] ", log.LstdFlags|log.Lmicroseconds)
	c := consumer.New(cfg, dec, output.New(os.Stdout), errLog)
	return c.Run(ctx)
}
