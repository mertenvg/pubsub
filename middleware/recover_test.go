package middleware

import (
	"context"
	"fmt"
	"reflect"
	"sync"
	"testing"

	"github.com/mertenvg/pubsub"
)

type PublishInfo struct {
	Topic string
	Data  string
	Key   string
}

type SubscribeInfo struct {
	Topic   string
	Handler pubsub.ProviderHandlerFunc
}

type MockProvider struct {
	mtx        sync.RWMutex
	Debug      bool
	PublishErr error
	StopErr    error
	Published  []PublishInfo
	Subscribed []SubscribeInfo
}

func (m *MockProvider) Publish(ctx context.Context, topic string, data []byte, key []byte) error {
	if m.Debug {
		fmt.Println(fmt.Sprintf("::: MockProvider.Publish() topic `%s` data `%s` key `%s`", topic, string(data), string(key)))
	}
	m.mtx.Lock()
	m.Published = append(m.Published, PublishInfo{
		Topic: topic,
		Data:  string(data),
		Key:   string(key),
	})
	err := m.PublishErr
	m.mtx.Unlock()
	return err
}

func (m *MockProvider) Subscribe(topic string, h pubsub.ProviderHandlerFunc) {
	if m.Debug {
		fmt.Println(fmt.Sprintf("::: MockProvider.Subscribe() topic `%s`", topic))
	}
	m.mtx.Lock()
	m.Subscribed = append(m.Subscribed, SubscribeInfo{
		Topic:   topic,
		Handler: h,
	})
	m.mtx.Unlock()
	return
}

func (m *MockProvider) Stop() error {
	m.mtx.Lock()
	m.Published = []PublishInfo{}
	m.Subscribed = []SubscribeInfo{}
	m.mtx.Unlock()
	return m.StopErr
}

func TestRecover(t *testing.T) {
	type args struct {
		f RecoveryHandlerFunc
	}
	tests := []struct {
		name    string
		handler pubsub.HandlerFunc
		args    args
		want    error
	}{
		{
			name: "recovers",
			handler: func(*pubsub.Context) error {
				panic(fmt.Errorf("panic at the disco"))
			},
			args: args{
				f: func(v any) error {
					return fmt.Errorf("recover from %v", v)
				},
			},
			want: fmt.Errorf("recover from %v", fmt.Errorf("panic at the disco")),
		},
		{
			name: "panic with error recovers with defaultRecoveryHandlerFunc",
			handler: func(*pubsub.Context) error {
				panic(fmt.Errorf("panic at the disco"))
			},
			args: args{
				f: nil,
			},
			want: fmt.Errorf("recovering panic in pubsub: %v", fmt.Errorf("panic at the disco")),
		},
		{
			name: "panic with string recovers with defaultRecoveryHandlerFunc",
			handler: func(*pubsub.Context) error {
				panic("panic at the disco")
			},
			args: args{
				f: nil,
			},
			want: fmt.Errorf("recovering panic in pubsub: %v", fmt.Errorf("panic at the disco")),
		},
		{
			name: "panic with something else recovers with defaultRecoveryHandlerFunc",
			handler: func(*pubsub.Context) error {
				panic(struct{}{})
			},
			args: args{
				f: nil,
			},
			want: fmt.Errorf("recovering panic in pubsub: %v", fmt.Errorf("unknown error")),
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := pubsub.NewContext(
				context.Background(),
				nil,
				pubsub.New(&MockProvider{}),
				"",
				[]pubsub.HandlerFunc{
					Recover(tt.args.f),
					tt.handler,
				},
			).Next()
			if !reflect.DeepEqual(got.Error(), tt.want.Error()) {
				t.Errorf("Recover() = %v, want %v", got, tt.want)
			}
		})
	}
}

func Test_defaultRecoveryHandler(t *testing.T) {
	type args struct {
		v any
	}
	tests := []struct {
		name    string
		args    args
		wantErr bool
	}{
		// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if err := defaultRecoveryHandler(tt.args.v); (err != nil) != tt.wantErr {
				t.Errorf("defaultRecoveryHandler() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}
