package pubsub

import (
	"context"
	"fmt"
	"reflect"
	"testing"
	"time"

	"github.com/sirupsen/logrus"
)

func TestWithLog(t *testing.T) {
	log := logrus.New()
	s := New(&MockProvider{})
	s.Configure(WithLog(log))
	if !reflect.DeepEqual(s.log, log) {
		t.Errorf("WithLog() got %v, want %v", s.log, log)
	}
}

func TestWithMiddlewares(t *testing.T) {
	mw := []HandlerFunc{
		func(ctx *Context) error { return nil },
	}
	s := New(&MockProvider{})
	s.Configure(WithMiddlewares(mw...))
	if fmt.Sprintf("%v", s.middleware) != fmt.Sprintf("%v", mw) {
		t.Errorf("WithMiddlewares() got %v, want %v", s.middleware, mw)
	}
}

type MockPlugin struct{}

func (m *MockPlugin) Middleware() HandlerFunc {
	return func(ctx *Context) error {
		return nil
	}
}
func (m *MockPlugin) Start(s *Service) {
	return
}
func (m *MockPlugin) Stop() error {
	return nil
}

func TestWithPlugin(t *testing.T) {
	p := &MockPlugin{}
	s := New(&MockProvider{})
	s.Configure(WithPlugin(p))
	if fmt.Sprintf("%v", s.plugins) != fmt.Sprintf("%v", []Plugin{p}) {
		t.Errorf("WithPlugin() got %v, want %v", s.plugins, []Plugin{p})
	}
	if len(s.middleware) != 1 {
		t.Errorf("WithPlugin() got middleware %v, want one middleware", s.middleware)
	}
}

func TestWithPublishHook(t *testing.T) {
	type args struct {
		topic string
		data  []byte
		key   string
	}
	var calledWith args
	hook := func(topic string, data []byte, key string) {
		calledWith = args{
			topic: topic,
			data:  data,
			key:   key,
		}
	}
	s := New(&MockProvider{})
	s.Configure(WithPublishHook(hook), WithPublishHook(nil))
	err := s.Publish(context.Background(), "a topic", "a message", "a key")
	if err != nil {
		t.Errorf("WithPublishHook() error = %v, wantErr false", err)
	}
	if calledWith.topic != "a topic" {
		t.Errorf("WithPublishHook() got topic = %v, want a topic", calledWith.topic)
	}
	if string(calledWith.data) != "a message" {
		t.Errorf("WithPublishHook() got data = %v, want a topic", string(calledWith.data))
	}
	if calledWith.key != "a key" {
		t.Errorf("WithPublishHook() got key = %v, want a topic", calledWith.key)
	}
}

func TestWithPublishRetryConfig(t *testing.T) {
	conf := &PubRetryConf{
		Limit:   100,
		Delay:   time.Millisecond,
		Backoff: defaultPublishRetryBackoff,
	}
	s := New(&MockProvider{})
	s.Configure(WithPublishRetryConfig(conf))
	if !reflect.DeepEqual(s.pubRetry, conf) {
		t.Errorf("WithPublishRetryConfig() got %v, want %v", s.pubRetry, conf)
	}
}

func TestWithSubscriber(t *testing.T) {
	s := New(&MockProvider{})
	s.Configure(WithSubscriber(&MockProvider{}))
	if len(s.subs) != 2 {
		t.Errorf("WithSubscriber() got subs = %v, want 2", len(s.subs))
	}
}

func TestWithWorkerLimit(t *testing.T) {
	s := New(&MockProvider{})
	s.Configure(WithWorkerLimit(2))
	if s.workers != 2 {
		t.Errorf("WithWorkerLimit() got = %v, want 2", s.workers)
	}
	s.Configure(WithWorkerLimit(0))
	if s.workers != 1 {
		t.Errorf("WithWorkerLimit() got = %v, want 1", s.workers)
	}
}
