package middleware

import (
	"errors"
	"fmt"

	"github.com/mertenvg/pubsub"
)

// RecoveryHandlerFunc handles recovery, func-ily
type RecoveryHandlerFunc func(v any) error

// defaultRecoveryHandler will be used if nil is provided to the Recover middleware for the RecoveryHandlerFunc
func defaultRecoveryHandler(v any) error {
	var err error
	switch t := v.(type) {
	case string:
		err = errors.New(t)
	case error:
		err = t
	default:
		err = errors.New("unknown error")
	}
	return fmt.Errorf("recovering panic in pubsub: %w", err)
}

// Recover catches any panics during the handling of events/messages and calls the
// provided (or default) RecoveryHandlerFunc and returns the error through the originating
// event/message handler
func Recover(f RecoveryHandlerFunc) pubsub.HandlerFunc {
	if f == nil {
		f = defaultRecoveryHandler
	}
	return func(ctx *pubsub.Context) (err error) {
		defer func() {
			if v := recover(); v != nil {
				err = f(v)
			}
		}()

		return ctx.Next()
	}
}
