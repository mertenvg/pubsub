package memory

import (
	"reflect"
	"testing"
)

func TestMessage_Ack(t *testing.T) {
	m := &Message{
		ack: make(chan empty, 1),
	}

	err := m.Ack()

	if err != nil {
		t.Errorf("Ack() returned unexpected error = %v", err)
	}

	if len(m.ack) < 1 {
		t.Errorf("Expected Ack() to push onto m.ack channel")
	}

	close(m.ack)
}

func TestMessage_Data(t *testing.T) {
	type fields struct {
		data []byte
	}
	tests := []struct {
		name   string
		fields fields
		want   []byte
	}{
		{
			name: "standard",
			fields: fields{
				data: []byte("this is a random message"),
			},
			want: []byte("this is a random message"),
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			m := &Message{
				data: tt.fields.data,
			}
			if got := m.Data(); !reflect.DeepEqual(got, tt.want) {
				t.Errorf("Data() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestMessage_Nack(t *testing.T) {
	m := &Message{
		nack: make(chan empty, 1),
	}

	err := m.Nack()

	if err != nil {
		t.Errorf("Nack() returned unexpected error = %v", err)
	}

	if len(m.nack) < 1 {
		t.Errorf("Expected Nack() to push onto m.nack channel")
	}

	close(m.nack)
}
