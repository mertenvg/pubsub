package pubsub

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/sirupsen/logrus"
)

func TestErrorList_Error(t *testing.T) {
	tests := []struct {
		name string
		el   ErrorList
		want string
	}{
		{
			name: "prints errors",
			el: ErrorList{
				fmt.Errorf("error one"),
				fmt.Errorf("error two"),
				fmt.Errorf("error three"),
				fmt.Errorf("error four"),
			},
			want: "[error one; error two; error three; error four]",
		},
		{
			name: "empty list",
			el:   ErrorList{},
			want: "",
		},
		{
			name: "nil list",
			el:   nil,
			want: "",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := tt.el.Error(); got != tt.want {
				t.Errorf("Error() = %v, want %v", got, tt.want)
			}
		})
	}
}

type MockPublisher struct {
	count     uint
	reject    uint
	lastTopic string
	lastData  []byte
	lastKey   []byte
}

func (m *MockPublisher) Publish(ctx context.Context, topic string, data []byte, key []byte) error {
	m.count++
	m.lastTopic = topic
	m.lastData = data
	m.lastKey = key
	if m.count <= m.reject {
		return fmt.Errorf("rejecting %v/%v", m.count, m.reject)
	}
	return nil
}

func ContextWithTimeout(ctx context.Context, t time.Duration) context.Context {
	ctx2, cancel := context.WithTimeout(ctx, t)
	defer func() {
		<-ctx2.Done()
		cancel()
	}()
	return ctx2
}

func TestService_publishWithRetry(t *testing.T) {
	log := logrus.New()
	type fields struct {
		pubRetry *PubRetryConf
		pub      Publisher
		log      logrus.FieldLogger
	}
	type args struct {
		ctx        context.Context
		topic      string
		data       []byte
		key        []byte
		attempt    uint
		retryDelay time.Duration
	}
	tests := []struct {
		name    string
		fields  fields
		args    args
		wantErr bool
	}{
		{
			name: "retry 6 times",
			fields: fields{
				pubRetry: &PubRetryConf{
					Limit:   10,
					Delay:   0,
					Backoff: defaultPublishRetryBackoff,
				},
				pub: &MockPublisher{
					reject: 5,
				},
				log: log,
			},
			args: args{
				ctx:        context.Background(),
				topic:      "firefly",
				data:       []byte("some data"),
				key:        []byte("a key"),
				attempt:    0,
				retryDelay: 0,
			},
			wantErr: false,
		},
		{
			name: "no retries",
			fields: fields{
				pubRetry: &PubRetryConf{
					Limit:   0,
					Delay:   0,
					Backoff: defaultPublishRetryBackoff,
				},
				pub: &MockPublisher{
					reject: 1,
				},
				log: log,
			},
			args: args{
				ctx:        context.Background(),
				topic:      "firefly",
				data:       []byte("some data"),
				key:        []byte("a key"),
				attempt:    0,
				retryDelay: 0,
			},
			wantErr: true,
		},
		{
			name: "can timeout",
			fields: fields{
				pubRetry: &PubRetryConf{
					Limit:   10,
					Delay:   time.Millisecond,
					Backoff: defaultPublishRetryBackoff,
				},
				pub: &MockPublisher{
					reject: 1,
				},
				log: log,
			},
			args: args{
				ctx:        ContextWithTimeout(context.Background(), time.Millisecond*3),
				topic:      "firefly",
				data:       []byte("some data"),
				key:        []byte("a key"),
				attempt:    0,
				retryDelay: 0,
			},
			wantErr: true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s := &Service{
				pubRetry: tt.fields.pubRetry,
				pub:      tt.fields.pub,
				log:      tt.fields.log,
			}
			if err := s.publishWithRetry(tt.args.ctx, tt.args.topic, tt.args.data, tt.args.key, tt.args.attempt, tt.args.retryDelay); (err != nil) != tt.wantErr {
				t.Errorf("publishWithRetry() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}

func Test_defaultPublishRetryBackoff(t *testing.T) {
	type args struct {
		last time.Duration
	}
	tests := []struct {
		name string
		args args
		want time.Duration
	}{
		{
			name: "duration multiplied by publishRetryBackoffMultiplier",
			args: args{
				time.Millisecond,
			},
			want: time.Duration(publishRetryBackoffMultiplier * float64(time.Millisecond)),
		},
		{
			name: "does not exceed publishRetryBackoffMax",
			args: args{
				publishRetryBackoffMax,
			},
			want: publishRetryBackoffMax,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := defaultPublishRetryBackoff(tt.args.last); got != tt.want {
				t.Errorf("defaultPublishRetryBackoff() = %v, want %v", got, tt.want)
			}
		})
	}
}
