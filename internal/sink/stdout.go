package sink

import (
	"context"
	"sync"

	"github.com/mayconrayone/consumer-kafka-go/internal/output"
)

// Stdout prints each decoded event as pretty JSON. It serializes writes so
// output from concurrent readers doesn't interleave.
type Stdout struct {
	mu      sync.Mutex
	printer *output.Printer
}

func NewStdout(p *output.Printer) *Stdout {
	return &Stdout{printer: p}
}

func (s *Stdout) Store(_ context.Context, evts []Event) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	for _, evt := range evts {
		if err := s.printer.Print(evt.Decoded); err != nil {
			return err
		}
	}
	return nil
}

func (s *Stdout) Close() error { return nil }
