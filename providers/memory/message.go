package memory

type empty struct{}

type Message struct {
	key  []byte
	data []byte
	ack  chan empty
	nack chan empty
}

func (m *Message) Key() []byte {
	return m.key
}

func (m *Message) Data() []byte {
	return m.data
}

func (m *Message) Ack() error {
	m.ack <- empty{}
	return nil
}

func (m *Message) Nack() error {
	m.nack <- empty{}
	return nil
}
