package kafka

import (
	"github.com/segmentio/kafka-go"
)

type Message struct {
	km kafka.Message
}

func NewMessage(km kafka.Message) *Message {
	return &Message{
		km: km,
	}
}

func (m *Message) Key() []byte {
	return m.km.Key
}

func (m *Message) Data() []byte {
	return m.km.Value
}

func (m *Message) Ack() error {
	// this implementation automatically ack's messages
	return nil
}

func (m *Message) Nack() error {
	// can't touch this. Messages are automatically ack'ed when read.
	return nil
}
