package output

import (
	"encoding/json"
	"fmt"
	"io"
)

type Printer struct {
	w io.Writer
}

func New(w io.Writer) *Printer {
	return &Printer{w: w}
}

func (p *Printer) Print(m map[string]any) error {
	b, err := json.MarshalIndent(m, "", "  ")
	if err != nil {
		return fmt.Errorf("marshal output: %w", err)
	}
	if _, err := p.w.Write(append(b, '\n')); err != nil {
		return fmt.Errorf("write output: %w", err)
	}
	return nil
}
