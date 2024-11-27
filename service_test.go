package pubsub

import (
	"bytes"
	"context"
	"fmt"
	"sync"
	"testing"
	"time"
)

type PublishInfo struct {
	Topic string
	Data  string
	Key   string
}

type SubscribeInfo struct {
	Topic   string
	Handler ProviderHandlerFunc
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

func (m *MockProvider) Subscribe(topic string, h ProviderHandlerFunc) {
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

func NewMiddleware(buf *bytes.Buffer, id string, err error, debug bool) HandlerFunc {
	return func(ctx *Context) error {
		buf.WriteString(id)
		if debug {
			fmt.Println(fmt.Sprintf("mw-%v", id))
		}
		return err
	}
}

func NewHandlerFunc(buf *bytes.Buffer, id string, err error, debug bool) HandlerFunc {
	return func(ctx *Context) error {
		buf.WriteString(id)
		if debug {
			fmt.Println(fmt.Sprintf("h-%v", id))
		}
		return err
	}
}

func TestNew(t *testing.T) {
	p := &MockProvider{}
	s := New(p)
	if s == nil {
		t.Errorf("New() got %v, want %v", s, &Service{})
	}
}

func TestService_Group(t *testing.T) {
	var buf bytes.Buffer
	var err error
	var debug bool

	p := &MockProvider{}
	s := New(p, WithMiddlewares(NewMiddleware(&buf, "1", nil, debug), NewMiddleware(&buf, "2", nil, debug)))
	gOne := s.Group(NewMiddleware(&buf, "3", nil, debug), NewMiddleware(&buf, "4", nil, debug))
	gTwo := s.Group(NewMiddleware(&buf, "3", nil, debug), NewMiddleware(&buf, "4", nil, debug))

	gOne.Subscribe("test-one", NewHandlerFunc(&buf, "5", nil, debug))
	gTwo.Subscribe("test-two", NewHandlerFunc(&buf, "5", nil, debug))

	err = s.Handle("test-one", NewMockMessage("msg", "key"))
	if err != nil {
		t.Errorf("Group() error = %v, wantErr false", err)
	}
	if calllog := buf.String(); calllog != "12345" {
		t.Errorf("Group() call log got = %v, want %v", calllog, "12345")
	}
	buf.Reset()

	err = s.Handle("test-two", NewMockMessage("msg", "key"))
	if err != nil {
		t.Errorf("Group() error = %v, wantErr false", err)
	}
	if calllog := buf.String(); calllog != "12345" {
		t.Errorf("Group() call log got = %v, want %v", calllog, "12345")
	}
	buf.Reset()
}

func TestGroup_GroupSubscribe(t *testing.T) {
	var buf bytes.Buffer
	var err error
	var debug bool

	p := &MockProvider{}
	s := New(p, WithMiddlewares(NewMiddleware(&buf, "1", nil, debug), NewMiddleware(&buf, "2", nil, debug)))
	gTwo := s.Group(NewMiddleware(&buf, "3", nil, debug), NewMiddleware(&buf, "4", nil, debug))
	gTwoPlus := gTwo.Group(NewMiddleware(&buf, "5", nil, debug), NewMiddleware(&buf, "6", nil, debug))
	gTwoPlus.Subscribe("test-two-plus", NewHandlerFunc(&buf, "7", nil, debug))

	err = s.Handle("test-two-plus", NewMockMessage("msg", "key"))
	if (err != nil) != false {
		t.Errorf("Group() error = %v, wantErr false", err)
	}
	if calllog := buf.String(); calllog != "1234567" {
		t.Errorf("Group() call log got = %v, want %v", calllog, "1234567")
	}
	buf.Reset()
}

func TestService_Handle(t *testing.T) {
	var buf bytes.Buffer
	var err error
	var debug bool
	var wantErr bool

	p := &MockProvider{}
	s := New(p)

	s.Subscribe("test", NewMiddleware(&buf, "1", nil, debug), NewHandlerFunc(&buf, "2", nil, debug))
	s.Subscribe("test-mw-fail", NewMiddleware(&buf, "1", fmt.Errorf("mw fail"), debug), NewHandlerFunc(&buf, "2", nil, debug))
	s.Subscribe("test-h-fail", NewMiddleware(&buf, "1", nil, debug), NewHandlerFunc(&buf, "2", fmt.Errorf("h fail"), debug))

	t.Run("test", func(t *testing.T) {
		err = s.Handle("test", NewMockMessage("msg", "key"))
		if (err != nil) != wantErr {
			t.Errorf("Handle() error = %v, wantErr %v", err, wantErr)
		}
		if calllog := buf.String(); calllog != "12" {
			t.Errorf("Handle() call log got = %v, want %v", calllog, "12")
		}
		buf.Reset()
	})

	wantErr = true

	t.Run("test-mw-fail", func(t *testing.T) {
		err = s.Handle("test-mw-fail", NewMockMessage("msg", "key"))
		if (err != nil) != wantErr {
			t.Errorf("Handle() error = %v, wantErr %v", err, wantErr)
		}
		if calllog := buf.String(); calllog != "1" {
			t.Errorf("Handle() call log got = %v, want %v", calllog, "1")
		}
		buf.Reset()
	})

	t.Run("test-h-fail", func(t *testing.T) {
		err = s.Handle("test-h-fail", NewMockMessage("msg", "key"))
		if (err != nil) != wantErr {
			t.Errorf("Handle() error = %v, wantErr %v", err, wantErr)
		}
		if calllog := buf.String(); calllog != "12" {
			t.Errorf("Handle() call log got = %v, want %v", calllog, "12")
		}
		buf.Reset()
	})
}

type StringMarshaler string

func (s StringMarshaler) MarshalMessage() ([]byte, error) {
	return []byte(s), nil
}

func TestService_Publish(t *testing.T) {
	var err error
	var wantErr bool

	p := &MockProvider{}
	s := New(p)
	ctx, cancel := context.WithCancel(context.Background())

	err = s.Publish(ctx, "test", NewMockMessage("some data", "a key"), "a key")
	if (err != nil) != wantErr {
		t.Errorf("Publish() error = %v, wantErr %v", err, wantErr)
	}

	err = s.Publish(ctx, "test", StringMarshaler("some data"), "a key")
	if (err != nil) != wantErr {
		t.Errorf("Publish() error = %v, wantErr %v", err, wantErr)
	}

	cancel()
}

func TestService_PublishAsync(t *testing.T) {
	p := &MockProvider{}
	s := New(p)
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	done := make(chan struct{})

	s.PublishAsync(ctx, "test", "some data", "a key")

	go func() {
		// wait for the message in MockProvider
		for i := 0; i < 1000; i++ {
			p.mtx.RLock()
			if len(p.Published) > 0 {
				msg := p.Published[0]
				if msg.Key == "a key" && msg.Data == "some data" && msg.Topic == "test" {
					done <- struct{}{}
					return
				}
			}
			p.mtx.RUnlock()
			time.Sleep(time.Millisecond)
		}
	}()

	var err error
	var messageSent bool
	var wantErr bool

	select {
	case <-ctx.Done():
		err = ctx.Err()
	case <-done:
		messageSent = true
	}

	cancel()
	close(done)

	if (err != nil) != wantErr {
		t.Errorf("PublishAsync() error = %v, wantErr %v", err, wantErr)
	}
	if !messageSent {
		t.Errorf("PublishAsync() message not sent, expected message to be sent")
	}
}

type MemoryMessage struct {
	key  []byte
	data []byte
}

func (m *MemoryMessage) Key() []byte  { return m.key }
func (m *MemoryMessage) Data() []byte { return m.data }
func (m *MemoryMessage) Ack() error   { return nil }
func (m *MemoryMessage) Nack() error  { return nil }

type MemoryProvider map[string][]ProviderHandlerFunc

func (p MemoryProvider) Publish(ctx context.Context, topic string, data []byte, key []byte) error {
	if handlers, ok := p[topic]; ok {
		msg := &MemoryMessage{
			key:  key,
			data: data,
		}
		for _, h := range handlers {
			h(msg)
		}
	}
	return nil
}

func (p MemoryProvider) Subscribe(topic string, handlerFunc ProviderHandlerFunc) {
	if handlerFunc == nil {
		delete(p, topic)
		return
	}
	p[topic] = append(p[topic], handlerFunc)
}

// Stop implements Subscriber
func (p MemoryProvider) Stop() error {
	for topic, _ := range p {
		delete(p, topic)
	}
	return nil
}

func TestService_SubscribeStopWait(t *testing.T) {
	var buf bytes.Buffer
	var err error
	var debug bool
	var wantErr bool

	p := MemoryProvider{}
	s := New(p)

	s.Subscribe("test", NewHandlerFunc(&buf, "1", nil, debug))

	err = s.Publish(context.Background(), "test", "a message", NewKey)
	if (err != nil) != wantErr {
		t.Errorf("SubscribeStopWait() error = %v, wantErr %v", err, wantErr)
	}

	err = s.Stop()
	if (err != nil) != wantErr {
		t.Errorf("SubscribeStopWait() error = %v, wantErr %v", err, wantErr)
	}

	s.Wait()
}
