package memory

import (
	"context"
	"reflect"
	"testing"

	"github.com/sirupsen/logrus"

	"github.com/mertenvg/pubsub"
)

func MockHandlerFunc(msg pubsub.Message) {
	_ = msg
}

func TestAdapter_PublishSimple(t *testing.T) {
	a := NewProvider()
	topic := "some-topic"
	var data string
	var key string
	a.Subscribe(topic, func(msg pubsub.Message) {
		data = string(msg.Data())
		key = string(msg.Key())
	})
	err := a.Publish(context.Background(), topic, []byte("some-value"), []byte("some-key"))
	if err != nil {
		t.Errorf("Unexpected error from a.Publish(topic, []byte(\"some-value\")): %s", err)
	}
	if data != "some-value" {
		t.Errorf("Unexpected data received by handler, expected 'some-value': got '%s'", data)
	}
	if key != "some-key" {
		t.Errorf("Unexpected key received by handler, expected 'some-key': got '%s'", key)
	}
}

func TestAdapter_PublishWithAck(t *testing.T) {
	a := NewProvider(WithAck(), WithLog(logrus.New()))
	topic := "some-topic"
	count := 0
	var data string
	var key string
	a.Subscribe(topic, func(msg pubsub.Message) {
		if count < 3 {
			_ = msg.Nack()
			count++
			return
		}
		data = string(msg.Data())
		key = string(msg.Key())
		_ = msg.Ack()
	})
	err := a.Publish(context.Background(), topic, []byte("some-value"), []byte("some-key"))
	if err != nil {
		t.Errorf("Unexpected error from a.Publish(topic, []byte(\"some-value\")): %s", err)
	}
	if count != 3 {
		t.Errorf("Expected handler to be called 3 times, got %v", count)
	}
	if data != "some-value" {
		t.Errorf("Unexpected data received by handler, expected 'some-value': got '%s'", data)
	}
	if key != "some-key" {
		t.Errorf("Unexpected key received by handler, expected 'some-key': got '%s'", key)
	}
}

func TestAdapter_Stop(t *testing.T) {
	a := NewProvider()
	topic := "some-topic"
	if len(a.subscribers) > 0 {
		t.Errorf("Expected at subscriber list to be empty at start")
	}
	a.Subscribe(topic, MockHandlerFunc)
	err := a.Stop()
	if err != nil {
		t.Errorf("Unexpected error from a.Stop(): %s", err)
	}
	if len(a.subscribers) > 0 {
		t.Errorf("Expected at subscriber list to be empty after a.Stop(), got %+v", a.subscribers)
	}
}

func TestAdapter_Subscribe(t *testing.T) {
	a := NewProvider()
	topic := "some-topic"
	if len(a.subscribers) > 0 {
		t.Errorf("Expected at subscriber list to be empty at start")
	}
	a.Subscribe(topic, MockHandlerFunc)
	if len(a.subscribers) == 0 {
		t.Errorf("Expected at least one handler in the subscriber list")
	}
	h, ok := a.subscribers[topic]
	if !ok {
		t.Errorf("Expected a handler for topic '%s' in subscriber list", topic)
	}
	if h == nil {
		t.Errorf("Expected handler for topic '%s' to be not nil", topic)
	}
	a.Subscribe(topic, nil)
	if len(a.subscribers) > 0 {
		t.Errorf("Expected at subscriber list to be empty after unsubscribe, got %+v", a.subscribers)
	}
}

func TestNewAdapter(t *testing.T) {
	type args struct {
		opts []Option
	}
	tests := []struct {
		name string
		args args
		want *Provider
	}{
		{
			name: "default",
			args: args{},
			want: &Provider{
				subscribers: map[string][]pubsub.ProviderHandlerFunc{},
			},
		},
		{
			name: "with ack",
			args: args{
				opts: []Option{WithAck()},
			},
			want: &Provider{
				requireAck:  true,
				subscribers: map[string][]pubsub.ProviderHandlerFunc{},
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := NewProvider(tt.args.opts...); !reflect.DeepEqual(got.requireAck, tt.want.requireAck) {
				t.Errorf("NewProvider().requireAck = %v, want %v", got.requireAck, tt.want.requireAck)
			}
		})
	}
}

func TestWithAck(t *testing.T) {
	a := NewProvider(WithAck())
	if !a.requireAck {
		t.Errorf("expected Provider to have requireAck set to true")
	}
}
