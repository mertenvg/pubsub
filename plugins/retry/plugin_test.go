package retry

import (
	"encoding/json"
	"reflect"
	"testing"

	"github.com/mertenvg/pubsub"
)

func TestNewPlugin(t *testing.T) {
	type args struct {
		retryTopicName      string
		deadLetterTopicName string
		maxAttempts         uint
	}
	tests := []struct {
		name string
		args args
		want *Plugin
	}{
		// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := NewPlugin(tt.args.retryTopicName, tt.args.deadLetterTopicName, tt.args.maxAttempts); !reflect.DeepEqual(got, tt.want) {
				t.Errorf("NewPlugin() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestPlugin_Middleware(t *testing.T) {
	type fields struct {
		retryTopicName      string
		deadLetterTopicName string
		maxAttempts         uint
	}
	tests := []struct {
		name   string
		fields fields
		want   pubsub.HandlerFunc
	}{
		// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			p := &Plugin{
				retryTopicName:      tt.fields.retryTopicName,
				deadLetterTopicName: tt.fields.deadLetterTopicName,
				maxAttempts:         tt.fields.maxAttempts,
			}
			if got := p.Middleware(); !reflect.DeepEqual(got, tt.want) {
				t.Errorf("Middleware() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestPlugin_Start(t *testing.T) {
	type fields struct {
		retryTopicName      string
		deadLetterTopicName string
		maxAttempts         uint
	}
	type args struct {
		s *pubsub.Service
	}
	tests := []struct {
		name   string
		fields fields
		args   args
	}{
		// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			p := &Plugin{
				retryTopicName:      tt.fields.retryTopicName,
				deadLetterTopicName: tt.fields.deadLetterTopicName,
				maxAttempts:         tt.fields.maxAttempts,
			}
			p.Start(tt.args.s)
		})
	}
}

func TestPlugin_Stop(t *testing.T) {
	type fields struct {
		retryTopicName      string
		deadLetterTopicName string
		maxAttempts         uint
	}
	tests := []struct {
		name    string
		fields  fields
		wantErr bool
	}{
		// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			p := &Plugin{
				retryTopicName:      tt.fields.retryTopicName,
				deadLetterTopicName: tt.fields.deadLetterTopicName,
				maxAttempts:         tt.fields.maxAttempts,
			}
			if err := p.Stop(); (err != nil) != tt.wantErr {
				t.Errorf("Stop() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}

func TestPlugin_retryQueueMessageHandler(t *testing.T) {
	type fields struct {
		retryTopicName      string
		deadLetterTopicName string
		maxAttempts         uint
	}
	tests := []struct {
		name   string
		fields fields
		want   pubsub.HandlerFunc
	}{
		// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			p := &Plugin{
				retryTopicName:      tt.fields.retryTopicName,
				deadLetterTopicName: tt.fields.deadLetterTopicName,
				maxAttempts:         tt.fields.maxAttempts,
			}
			if got := p.retryQueueMessageHandler(); !reflect.DeepEqual(got, tt.want) {
				t.Errorf("retryQueueMessageHandler() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestWrapper_Ack(t *testing.T) {
	type fields struct {
		OriginTopic string
		OriginKey   string
		Message     json.RawMessage
		Attempts    uint
	}
	tests := []struct {
		name    string
		fields  fields
		wantErr bool
	}{
		// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			rw := &Wrapper{
				OriginTopic: tt.fields.OriginTopic,
				OriginKey:   tt.fields.OriginKey,
				Message:     tt.fields.Message,
				Attempts:    tt.fields.Attempts,
			}
			if err := rw.Ack(); (err != nil) != tt.wantErr {
				t.Errorf("Ack() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}

func TestWrapper_Data(t *testing.T) {
	type fields struct {
		OriginTopic string
		OriginKey   string
		Message     json.RawMessage
		Attempts    uint
	}
	tests := []struct {
		name   string
		fields fields
		want   []byte
	}{
		// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			rw := &Wrapper{
				OriginTopic: tt.fields.OriginTopic,
				OriginKey:   tt.fields.OriginKey,
				Message:     tt.fields.Message,
				Attempts:    tt.fields.Attempts,
			}
			if got := rw.Data(); !reflect.DeepEqual(got, tt.want) {
				t.Errorf("Data() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestWrapper_Key(t *testing.T) {
	type fields struct {
		OriginTopic string
		OriginKey   string
		Message     json.RawMessage
		Attempts    uint
	}
	tests := []struct {
		name   string
		fields fields
		want   []byte
	}{
		// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			rw := &Wrapper{
				OriginTopic: tt.fields.OriginTopic,
				OriginKey:   tt.fields.OriginKey,
				Message:     tt.fields.Message,
				Attempts:    tt.fields.Attempts,
			}
			if got := rw.Key(); !reflect.DeepEqual(got, tt.want) {
				t.Errorf("Key() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestWrapper_Nack(t *testing.T) {
	type fields struct {
		OriginTopic string
		OriginKey   string
		Message     json.RawMessage
		Attempts    uint
	}
	tests := []struct {
		name    string
		fields  fields
		wantErr bool
	}{
		// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			rw := &Wrapper{
				OriginTopic: tt.fields.OriginTopic,
				OriginKey:   tt.fields.OriginKey,
				Message:     tt.fields.Message,
				Attempts:    tt.fields.Attempts,
			}
			if err := rw.Nack(); (err != nil) != tt.wantErr {
				t.Errorf("Nack() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}
