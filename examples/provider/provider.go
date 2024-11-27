package provider

import (
	"github.com/mertenvg/pubsub"
)

// Provider implements pubsub.Provider
type Provider struct{}

// NewProvider implements pubsub.Provider
func NewProvider() *Provider {
	return &Provider{}
}

// Publish implements pubsub.Publisher
func (p *Provider) Publish(topic string, data []byte, key []byte) error {
	// ...
	return nil
}

// Subscribe implements pubsub.Subscriber
func (p *Provider) Subscribe(topic string, handlerFunc pubsub.ProviderHandlerFunc) {
	// ...
	handlerFunc(&Message{})
	// ...
}

// Stop implements pubsub.Subscriber
func (p *Provider) Stop() error {
	return nil
}

// ----------------------------------------

// Message implements pubsub.Message
type Message struct{}

// Key implements pubsub.Message
func (m *Message) Key() []byte {
	return nil
}

// Data implements pubsub.Message
func (m *Message) Data() []byte {
	return nil
}

// Ack implements pubsub.Message
func (m *Message) Ack() error {
	return nil
}

// Nack implements pubsub.Message
func (m *Message) Nack() error {
	return nil
}
