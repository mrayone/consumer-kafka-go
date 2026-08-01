// Package logging builds the shared logrus logger from the configured level.
package logging

import (
	"os"

	"github.com/sirupsen/logrus"

	"github.com/mayconrayone/consumer-kafka-go/internal/config"
)

// New returns a logrus logger writing to stderr (stdout is reserved for decoded
// message output) at the given config log level: error | info | debug.
func New(level string) *logrus.Logger {
	l := logrus.New()
	l.SetOutput(os.Stderr)
	l.SetFormatter(&logrus.TextFormatter{FullTimestamp: true})
	switch level {
	case config.LogError:
		l.SetLevel(logrus.ErrorLevel)
	case config.LogDebug:
		l.SetLevel(logrus.DebugLevel)
	default:
		l.SetLevel(logrus.InfoLevel)
	}
	return l
}
