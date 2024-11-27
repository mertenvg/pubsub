package pubsub

import "context"

type ServiceOption func(k *Service)

type HandlerFunc func(ctx *Context) error

type PublishHook func(topic string, data []byte, key string)

type Message interface {
	Key() []byte
	Data() []byte
	Ack() error
	Nack() error
}

type Plugin interface {
	Middleware() HandlerFunc
	Start(s *Service)
	Stop() error
}

type ProviderHandlerFunc func(msg Message)

type Publisher interface {
	Publish(ctx context.Context, topic string, data []byte, key []byte) error
}

type Subscriber interface {
	Subscribe(topic string, h ProviderHandlerFunc)
	Stop() error
}

type Provider interface {
	Publisher
	Subscriber
}

type Marshaler interface {
	MarshalMessage() ([]byte, error)
}

type Unmarshaler interface {
	UnmarshalMessage([]byte) error
}
