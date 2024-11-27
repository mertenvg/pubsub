package retry

import (
	"encoding/json"
	"errors"

	"github.com/mertenvg/pubsub"
)

const (
	IsRetryMessageContextKey = "IsRetryMessageContextKey"
)

var (
	Error = errors.New("cannot handle message, please retry")
)

type Wrapper struct {
	OriginTopic string
	OriginKey   string
	Message     json.RawMessage
	Attempts    uint
}

func (rw *Wrapper) Key() []byte {
	return []byte(rw.OriginKey)
}

func (rw *Wrapper) Data() []byte {
	return []byte(rw.Message)
}

func (rw *Wrapper) Ack() error {
	return nil
}

func (rw *Wrapper) Nack() error {
	return nil
}

type Plugin struct {
	retryTopicName      string
	deadLetterTopicName string
	maxAttempts         uint
}

// NewPlugin creates a new logrus.Plugin
func NewPlugin(retryTopicName, deadLetterTopicName string, maxAttempts uint) *Plugin {
	return &Plugin{
		retryTopicName:      retryTopicName,
		deadLetterTopicName: deadLetterTopicName,
		maxAttempts:         maxAttempts,
	}
}

// Middleware implements pubsub.Plugin
func (p *Plugin) Middleware() pubsub.HandlerFunc {
	return func(ctx *pubsub.Context) error {
		// send it through to the handlers
		err := ctx.Next()

		// check the Context to see if this message is a second/... attempt of a previous message
		isRetrying, _ := ctx.GetBool(IsRetryMessageContextKey)

		// If the error is not nil and is a retry.Error and we're not already in a second/... attempt,
		// then it's safe to pass this message on to the retry queue
		if err != nil && errors.Is(err, Error) && !isRetrying {
			return ctx.Publish(p.retryTopicName, &Wrapper{
				OriginTopic: ctx.Topic(),
				OriginKey:   ctx.Key(),
				Message:     ctx.Raw(),
				Attempts:    1,
			}, ctx.Key())
		}

		// otherwise just return the possible error from the handlers
		return err
	}
}

// Start implements pubsub.Plugin
func (p *Plugin) Start(s *pubsub.Service) {
	// subscribe to the retry topic, so we can grab previous failed message and try them again
	s.Subscribe(p.retryTopicName, p.retryQueueMessageHandler())
}

// Stop implements pubsub.Plugin
func (p *Plugin) Stop() error {
	// subscription is stopped by service already through the providers
	// nothing else needs to be stopped for this plugin
	return nil
}

// retryQueueMessageHandler is called when we get a message back on the retry queue
func (p *Plugin) retryQueueMessageHandler() pubsub.HandlerFunc {
	return func(ctx *pubsub.Context) error {
		var retry Wrapper
		err := ctx.Bind(&retry)
		if err != nil {
			// this does not appear to be a retry message
			return err
		}
		err = ctx.Handle(retry.OriginTopic, &retry, pubsub.With(IsRetryMessageContextKey, true))
		if errors.Is(err, Error) {
			// if we've reached out max attempts limit,
			// then send the message to the dead letter queue and move on
			if retry.Attempts+1 > p.maxAttempts {
				return ctx.Publish(p.deadLetterTopicName, retry, retry.OriginKey)
			}
			// send the message back into the retry queue
			retry.Attempts++
			return ctx.Publish(p.retryTopicName, retry, retry.OriginKey)
		}
		return err
	}
}
