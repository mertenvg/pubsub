package middleware

import (
	"github.com/sirupsen/logrus"

	"github.com/mertenvg/pubsub"
)

const LogContextKey = "pubsub.LogContextKey"

// Logrus sets the message key and topic fields on the logger and adds it to the Context
func Logrus(log logrus.FieldLogger) pubsub.HandlerFunc {
	return func(ctx *pubsub.Context) error {
		ctx.Set(LogContextKey, log.WithFields(logrus.Fields{
			"key":   string(ctx.Key()),
			"topic": ctx.Topic(),
		}))

		return ctx.Next()
	}
}
