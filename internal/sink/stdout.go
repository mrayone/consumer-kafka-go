package sink

import (
	"context"

	"github.com/mayconrayone/consumer-kafka-go/internal/output"
)

// Stdout prints each decoded event as pretty JSON.
type Stdout struct {
	printer *output.Printer
}

func NewStdout(p *output.Printer) *Stdout {
	return &Stdout{printer: p}
}

func (s *Stdout) Store(_ context.Context, evt Event) error {
	return s.printer.Print(evt.Decoded)
}

func (s *Stdout) Close() error { return nil }
