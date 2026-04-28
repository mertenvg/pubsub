package valkey

import (
	"reflect"
	"testing"
)

func TestMessage_Key(t *testing.T) {
	tests := []struct {
		name string
		key  []byte
		want []byte
	}{
		{name: "standard", key: []byte("some-key"), want: []byte("some-key")},
		{name: "empty", key: []byte{}, want: []byte{}},
		{name: "nil", key: nil, want: nil},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			m := &Message{key: tt.key}
			if got := m.Key(); !reflect.DeepEqual(got, tt.want) {
				t.Errorf("Key() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestMessage_Data(t *testing.T) {
	tests := []struct {
		name string
		data []byte
		want []byte
	}{
		{name: "standard", data: []byte("some data"), want: []byte("some data")},
		{name: "empty", data: []byte{}, want: []byte{}},
		{name: "nil", data: nil, want: nil},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			m := &Message{data: tt.data}
			if got := m.Data(); !reflect.DeepEqual(got, tt.want) {
				t.Errorf("Data() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestMessage_Ack_NoClient(t *testing.T) {
	m := &Message{}
	err := m.Ack()
	if err != nil {
		t.Errorf("Ack() with nil client should return nil, got %v", err)
	}
}

func TestMessage_Nack(t *testing.T) {
	m := &Message{}
	err := m.Nack()
	if err != nil {
		t.Errorf("Nack() should return nil, got %v", err)
	}
}
