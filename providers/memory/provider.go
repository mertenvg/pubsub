package memory

import (
	"context"

	"github.com/sirupsen/logrus"

	"github.com/mertenvg/pubsub"
)

type Option func(p *Provider)

func WithAck() Option {
	return func(p *Provider) {
		p.requireAck = true
	}
}

func WithLog(log logrus.FieldLogger) Option {
	return func(p *Provider) {
		p.log = log
	}
}

// Provider implements pubsub.Provider
type Provider struct {
	requireAck  bool
	subscribers map[string][]pubsub.ProviderHandlerFunc
	log         logrus.FieldLogger
}

// NewProvider creates a new memory.Provider
func NewProvider(opts ...Option) *Provider {
	p := &Provider{
		subscribers: make(map[string][]pubsub.ProviderHandlerFunc),
		log:         logrus.New(),
	}

	for _, o := range opts {
		o(p)
	}

	p.log = p.log.WithField("scope", "pubsub memory provider")

	return p
}

// Publish implements pubsub.Publisher
func (p *Provider) Publish(ctx context.Context, topic string, data []byte, key []byte) error {
	fields := logrus.Fields{
		"topic":    topic,
		"data len": len(data),
		"key":      string(key),
	}
	p.log.WithFields(fields).Debug("look for topic subscription")
	if handlers, ok := p.subscribers[topic]; ok {
		p.log.WithFields(fields).Debug("wrap data in message struct")
		msg := &Message{
			key:  key,
			data: data,
			ack:  make(chan empty, 1),
			nack: make(chan empty, 1),
		}

		p.log.WithFields(fields).Debug("call the handler for this topic")
		for _, h := range handlers {
			h(msg)
		}

		if p.requireAck {
			var acked bool
			p.log.WithFields(fields).Debug("wait for ack or nack")
			select {
			case <-msg.ack:
				acked = true
			case <-msg.nack:
				acked = false
			}
			if !acked {
				p.log.WithFields(fields).Debug("message not acked - publish again")
				return p.Publish(ctx, topic, data, key)
			}
		}

		close(msg.ack)
		close(msg.nack)
	}
	return nil
}

// Subscribe implements pubsub.Subscriber
func (p *Provider) Subscribe(topic string, handlerFunc pubsub.ProviderHandlerFunc) {
	fields := logrus.Fields{
		"topic": topic,
	}
	if handlerFunc == nil {
		p.log.WithFields(fields).Debug("remove topic handler")
		delete(p.subscribers, topic)
		return
	}
	p.log.WithFields(fields).Debug("set topic handler")
	p.subscribers[topic] = append(p.subscribers[topic], handlerFunc)
}

// Stop implements pubsub.Subscriber
func (p *Provider) Stop() error {
	p.log.Debug("stop provider - clear all registered handlers")
	p.subscribers = make(map[string][]pubsub.ProviderHandlerFunc)
	return nil
}
