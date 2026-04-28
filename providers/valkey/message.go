package valkey

import (
	"context"

	vk "github.com/valkey-io/valkey-go"
)

// Message implements pubsub.Message for both pub/sub and streams modes.
type Message struct {
	key  []byte
	data []byte
	// streams-only fields for XACK
	streamID string
	topic    string
	group    string
	client   vk.Client
}

func (m *Message) Key() []byte {
	return m.key
}

func (m *Message) Data() []byte {
	return m.data
}

func (m *Message) Ack() error {
	if m.client == nil {
		return nil
	}
	return m.client.Do(context.Background(),
		m.client.B().Xack().Key(m.topic).Group(m.group).Id(m.streamID).Build(),
	).Error()
}

func (m *Message) Nack() error {
	// In streams mode the message stays pending for redelivery.
	// In pub/sub mode this is a no-op.
	return nil
}
