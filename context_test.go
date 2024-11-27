package pubsub

import (
	"context"
	"encoding/json"
	"fmt"
	"reflect"
	"testing"
	"time"
)

func NewMockMessage(data, key string) *MockMessage {
	return &MockMessage{
		data: []byte(data),
		key:  []byte(key),
	}
}

type MockMessage struct {
	data   []byte
	key    []byte
	acked  bool
	nacked bool
}

func (m *MockMessage) Key() []byte {
	return m.key
}
func (m *MockMessage) Data() []byte {
	return m.data
}
func (m *MockMessage) Ack() error {
	m.acked = true
	return nil
}
func (m *MockMessage) Nack() error {
	m.nacked = true
	return nil
}

type MockHook struct {
	acked  bool
	nacked bool
}

func (m *MockHook) Ack(ctx *Context) {
	m.acked = true
}

func (m *MockHook) Nack(ctx *Context) {
	m.nacked = true
}

type StringUnmarshaler string

func (s *StringUnmarshaler) UnmarshalMessage(data []byte) error {
	*s = StringUnmarshaler(data)
	return nil
}

func TestContext_Abort(t *testing.T) {
	var debug bool
	type fields struct {
		parent   context.Context
		msg      *MockMessage
		s        *Service
		topic    string
		handlers []HandlerFunc
	}
	tests := []struct {
		name     string
		fields   fields
		validate func(*Context, *testing.T)
		wantErr  bool
	}{
		{
			name: "abort from middleware",
			fields: fields{
				parent: context.Background(),
				msg:    NewMockMessage("message body", "message key"),
				s:      New(&MockProvider{}),
				topic:  "test",
				handlers: []HandlerFunc{
					func(ctx *Context) error {
						if debug {
							fmt.Println("::: Abort() handler 1")
						}
						return nil
					},
					func(ctx *Context) error {
						if debug {
							fmt.Println("::: Abort() handler 2")
						}
						ctx.Abort()
						return nil
					},
					func(ctx *Context) error {
						if debug {
							fmt.Println("::: Abort() handler 3")
						}
						return nil
					},
					func(ctx *Context) error {
						if debug {
							fmt.Println("::: Abort() handler 4")
						}
						return nil
					},
				},
			},
			validate: func(ctx *Context, t *testing.T) {
				if !ctx.aborted.Load() {
					t.Errorf("Context.Abort() expected ctx to be aborted")
				}
				// the index will be one greater than expected because the index is incremented before
				// checking if ctx was aborted.
				if ctx.index != 2 {
					t.Errorf("Context.Abort() expected first 2 handlers to have been called, got %v", ctx.index)
				}
			},
			wantErr: false,
		},
		{
			name: "abort from context timeout",
			fields: fields{
				parent: ContextWithTimeout(context.Background(), 0),
				msg:    NewMockMessage("message body", "message key"),
				s:      New(&MockProvider{}),
				topic:  "test",
				handlers: []HandlerFunc{
					func(ctx *Context) error {
						if debug {
							fmt.Println("::: Abort() handler 1")
						}
						time.Sleep(time.Millisecond)
						return nil
					},
					func(ctx *Context) error {
						if debug {
							fmt.Println("::: Abort() handler 2")
						}
						time.Sleep(time.Millisecond)
						return nil
					},
					func(ctx *Context) error {
						if debug {
							fmt.Println("::: Abort() handler 3")
						}
						time.Sleep(time.Millisecond)
						return nil
					},
					func(ctx *Context) error {
						if debug {
							fmt.Println("::: Abort() handler 4")
						}
						time.Sleep(time.Millisecond)
						return nil
					},
				},
			},
			validate: func(ctx *Context, t *testing.T) {
				if !ctx.aborted.Load() {
					t.Errorf("Context.Abort() expected ctx to be aborted")
				}
			},
			wantErr: true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ctx := NewContext(
				tt.fields.parent,
				tt.fields.msg,
				tt.fields.s,
				tt.fields.topic,
				tt.fields.handlers,
			)
			err := ctx.Next()
			if (err != nil) != tt.wantErr {
				t.Errorf("Context.Abort() error = %v, wantErr %v", err, tt.wantErr)
			}
			tt.validate(ctx, t)
		})
	}
}

func TestContext_Ack(t *testing.T) {
	var debug bool
	type fields struct {
		parent   context.Context
		msg      *MockMessage
		s        *Service
		topic    string
		handlers []HandlerFunc
	}
	tests := []struct {
		name     string
		fields   fields
		validate func(*Context, fields, *testing.T)
		wantErr  bool
	}{
		{
			name: "ack the message",
			fields: fields{
				parent: context.Background(),
				msg:    NewMockMessage("message body", "message key"),
				s:      New(&MockProvider{}),
				topic:  "test",
				handlers: []HandlerFunc{
					func(ctx *Context) error {
						if debug {
							fmt.Println("::: Ack() handler 1")
						}
						return nil
					},
					func(ctx *Context) error {
						if debug {
							fmt.Println("::: Ack() handler 2")
						}
						return ctx.Ack()
					},
				},
			},
			validate: func(ctx *Context, f fields, t *testing.T) {
				if !f.msg.acked {
					t.Errorf("Context.Ack() expected msg to have been acked")
				}
			},
			wantErr: false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ctx := NewContext(
				tt.fields.parent,
				tt.fields.msg,
				tt.fields.s,
				tt.fields.topic,
				tt.fields.handlers,
			)
			err := ctx.Next()
			if (err != nil) != tt.wantErr {
				t.Errorf("Context.Ack() error = %v, wantErr %v", err, tt.wantErr)
			}
			tt.validate(ctx, tt.fields, t)
		})
	}
}

func TestContext_Bind(t *testing.T) {
	{
		ctx := NewContext(
			context.Background(),
			NewMockMessage("message body", "message key"),
			New(&MockProvider{}),
			"test",
			nil,
		)
		{
			var got string
			if err := ctx.Bind(&got); (err != nil) != false {
				t.Errorf("Bind() error = %v, wantErr %v", err, false)
			}
			want := "message body"
			if got != want {
				t.Errorf("Bind() got = %v, want %v", got, want)
			}
		}
		{
			var got string
			if err := ctx.Bind(got); (err != nil) != true {
				t.Errorf("Bind() error = %v, wantErr %v", err, true)
			}
		}
		{
			var got StringUnmarshaler
			if err := ctx.Bind(&got); (err != nil) != false {
				t.Errorf("Bind() error = %v, wantErr %v", err, false)
			}
			want := "message body"
			if string(got) != want {
				t.Errorf("Bind() got = %v, want %v", got, want)
			}
		}
	}
	{
		type Msg struct {
			One   int    `json:"one"`
			Two   uint   `json:"two"`
			Three string `json:"three"`
		}
		want := Msg{One: 1, Two: 2, Three: "3"}
		msgBody, _ := json.Marshal(want)
		ctx := NewContext(
			context.Background(),
			NewMockMessage(string(msgBody), "json message"),
			New(&MockProvider{}),
			"test",
			nil,
		)
		{
			var got Msg
			if err := ctx.Bind(&got); (err != nil) != false {
				t.Errorf("Bind() error = %v, wantErr %v", err, false)
			}
			if !reflect.DeepEqual(got, want) {
				t.Errorf("Bind() got = %v, want %v", got, want)
			}
		}
	}
}

func TestContext_Configure(t *testing.T) {
	type fields struct {
		parent   context.Context
		msg      *MockMessage
		s        *Service
		topic    string
		handlers []HandlerFunc
	}
	type args struct {
		options []ContextOption
	}
	tests := []struct {
		name   string
		fields fields
		args   args
	}{
		{
			name: "configure context",
			fields: fields{
				parent: context.Background(),
				msg:    NewMockMessage("message body", "message key"),
				s:      New(&MockProvider{}),
				topic:  "test",
				handlers: []HandlerFunc{
					func(ctx *Context) error { fmt.Println("::: Ack() handler 1"); return nil },
					func(ctx *Context) error { fmt.Println("::: Ack() handler 2"); return ctx.Ack() },
				},
			},
			args: args{
				options: []ContextOption{
					With("one", 100),
					With("two", 200.00),
					With("three", "300"),
				},
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ctx := NewContext(
				tt.fields.parent,
				tt.fields.msg,
				tt.fields.s,
				tt.fields.topic,
				tt.fields.handlers,
			)
			ctx.Configure(tt.args.options...)
			if got, ok := ctx.Get("one"); !ok || !reflect.DeepEqual(got, 100) {
				t.Errorf("Configure() = %v, want %v", got, 100)
			}
			if got, ok := ctx.Get("two"); !ok || !reflect.DeepEqual(got, 200.00) {
				t.Errorf("Configure() = %v, want %v", got, 200.00)
			}
			if got, ok := ctx.Get("three"); !ok || !reflect.DeepEqual(got, "300") {
				t.Errorf("Configure() = %v, want %v", got, "300")
			}
		})
	}
}

func TestContext_Err(t *testing.T) {
	tests := []struct {
		name    string
		ctx     *Context
		wantErr bool
	}{
		{
			name:    "no err",
			ctx:     NewContext(context.Background(), nil, New(&MockProvider{}), "", nil),
			wantErr: false,
		},
		{
			name: "custom err",
			ctx: func() *Context {
				ctx := NewContext(context.Background(), nil, New(&MockProvider{}), "", nil)
				ctx.err = fmt.Errorf("some random error")
				return ctx
			}(),
			wantErr: true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if err := tt.ctx.Err(); (err != nil) != tt.wantErr {
				t.Errorf("Err() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}

func TestContext_Get(t *testing.T) {
	ctx := NewContext(
		context.Background(),
		NewMockMessage("message body", "message key"),
		New(&MockProvider{}),
		"test",
		[]HandlerFunc{
			func(ctx *Context) error { fmt.Println("::: Ack() handler 1"); return nil },
			func(ctx *Context) error { fmt.Println("::: Ack() handler 2"); return ctx.Ack() },
		},
	)
	ctx.Configure(
		With("one", 100),
		With("two", 200.00),
		With("three", "300"),
	)
	tests := []struct {
		name      string
		ctx       *Context
		key       string
		wantValue any
		wantOk    bool
	}{
		{
			name:      "one",
			ctx:       ctx,
			key:       "one",
			wantValue: 100,
			wantOk:    true,
		},
		{
			name:      "two",
			ctx:       ctx,
			key:       "two",
			wantValue: 200.00,
			wantOk:    true,
		},
		{
			name:      "three",
			ctx:       ctx,
			key:       "three",
			wantValue: "300",
			wantOk:    true,
		},
		{
			name:      "four",
			ctx:       ctx,
			key:       "four",
			wantValue: nil,
			wantOk:    false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ctx := tt.ctx
			gotValue, gotOk := ctx.Get(tt.key)
			if !reflect.DeepEqual(gotValue, tt.wantValue) {
				t.Errorf("Get() gotValue = %v, want %v", gotValue, tt.wantValue)
			}
			if gotOk != tt.wantOk {
				t.Errorf("Get() gotOk = %v, want %v", gotOk, tt.wantOk)
			}
		})
	}
}

func TestContext_GetBool(t *testing.T) {
	ctx := NewContext(
		context.Background(),
		NewMockMessage("message body", "message key"),
		New(&MockProvider{}),
		"test",
		[]HandlerFunc{
			func(ctx *Context) error { fmt.Println("::: GetBool() handler 1"); return nil },
			func(ctx *Context) error { fmt.Println("::: GetBool() handler 2"); return ctx.Ack() },
		},
	)
	ctx.Configure(
		With("yes", true),
		With("no", false),
	)
	tests := []struct {
		name      string
		ctx       *Context
		key       string
		wantValue bool
		wantOk    bool
	}{
		{
			name:      "yes",
			ctx:       ctx,
			key:       "yes",
			wantValue: true,
			wantOk:    true,
		},
		{
			name:      "no",
			ctx:       ctx,
			key:       "no",
			wantValue: false,
			wantOk:    true,
		},
		{
			name:      "not set",
			ctx:       ctx,
			key:       "other",
			wantValue: false,
			wantOk:    false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ctx := tt.ctx
			gotValue, gotOk := ctx.GetBool(tt.key)
			if !reflect.DeepEqual(gotValue, tt.wantValue) {
				t.Errorf("GetBool() gotValue = %v, want %v", gotValue, tt.wantValue)
			}
			if gotOk != tt.wantOk {
				t.Errorf("GetBool() gotOk = %v, want %v", gotOk, tt.wantOk)
			}
		})
	}
}

func TestContext_GetFloat(t *testing.T) {
	ctx := NewContext(
		context.Background(),
		NewMockMessage("message body", "message key"),
		New(&MockProvider{}),
		"test",
		[]HandlerFunc{
			func(ctx *Context) error { fmt.Println("::: GetFloat() handler 1"); return nil },
			func(ctx *Context) error { fmt.Println("::: GetFloat() handler 2"); return ctx.Ack() },
		},
	)
	ctx.Configure(
		With("123.123", 123.123),
		With("0.0", 0.0),
	)
	tests := []struct {
		name      string
		ctx       *Context
		key       string
		wantValue float64
		wantOk    bool
	}{
		{
			name:      "123.123",
			ctx:       ctx,
			key:       "123.123",
			wantValue: 123.123,
			wantOk:    true,
		},
		{
			name:      "0.0",
			ctx:       ctx,
			key:       "0.0",
			wantValue: 0.0,
			wantOk:    true,
		},
		{
			name:      "not set",
			ctx:       ctx,
			key:       "other",
			wantValue: 0.0,
			wantOk:    false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ctx := tt.ctx
			gotValue, gotOk := ctx.GetFloat(tt.key)
			if !reflect.DeepEqual(gotValue, tt.wantValue) {
				t.Errorf("GetFloat() gotValue = %v, want %v", gotValue, tt.wantValue)
			}
			if gotOk != tt.wantOk {
				t.Errorf("GetFloat() gotOk = %v, want %v", gotOk, tt.wantOk)
			}
		})
	}
}

func TestContext_GetInt(t *testing.T) {
	ctx := NewContext(
		context.Background(),
		NewMockMessage("message body", "message key"),
		New(&MockProvider{}),
		"test",
		[]HandlerFunc{
			func(ctx *Context) error { fmt.Println("::: GetInt() handler 1"); return nil },
			func(ctx *Context) error { fmt.Println("::: GetInt() handler 2"); return ctx.Ack() },
		},
	)
	ctx.Configure(
		With("123", 123),
		With("0", 0),
	)
	tests := []struct {
		name      string
		ctx       *Context
		key       string
		wantValue int
		wantOk    bool
	}{
		{
			name:      "123",
			ctx:       ctx,
			key:       "123",
			wantValue: 123,
			wantOk:    true,
		},
		{
			name:      "0",
			ctx:       ctx,
			key:       "0",
			wantValue: 0,
			wantOk:    true,
		},
		{
			name:      "not set",
			ctx:       ctx,
			key:       "other",
			wantValue: 0,
			wantOk:    false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ctx := tt.ctx
			gotValue, gotOk := ctx.GetInt(tt.key)
			if !reflect.DeepEqual(gotValue, tt.wantValue) {
				t.Errorf("GetInt() gotValue = %v, want %v", gotValue, tt.wantValue)
			}
			if gotOk != tt.wantOk {
				t.Errorf("GetInt() gotOk = %v, want %v", gotOk, tt.wantOk)
			}
		})
	}
}

func TestContext_GetString(t *testing.T) {
	ctx := NewContext(
		context.Background(),
		NewMockMessage("message body", "message key"),
		New(&MockProvider{}),
		"test",
		[]HandlerFunc{
			func(ctx *Context) error { fmt.Println("::: GetString() handler 1"); return nil },
			func(ctx *Context) error { fmt.Println("::: GetString() handler 2"); return ctx.Ack() },
		},
	)
	ctx.Configure(
		With("123.123", "123.123"),
		With("0.0", "0.0"),
	)
	tests := []struct {
		name      string
		ctx       *Context
		key       string
		wantValue string
		wantOk    bool
	}{
		{
			name:      "123.123",
			ctx:       ctx,
			key:       "123.123",
			wantValue: "123.123",
			wantOk:    true,
		},
		{
			name:      "0.0",
			ctx:       ctx,
			key:       "0.0",
			wantValue: "0.0",
			wantOk:    true,
		},
		{
			name:      "not set",
			ctx:       ctx,
			key:       "other",
			wantValue: "",
			wantOk:    false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ctx := tt.ctx
			gotValue, gotOk := ctx.GetString(tt.key)
			if !reflect.DeepEqual(gotValue, tt.wantValue) {
				t.Errorf("GetString() gotValue = %v, want %v", gotValue, tt.wantValue)
			}
			if gotOk != tt.wantOk {
				t.Errorf("GetString() gotOk = %v, want %v", gotOk, tt.wantOk)
			}
		})
	}
}

func TestContext_GetUint(t *testing.T) {
	ctx := NewContext(
		context.Background(),
		NewMockMessage("message body", "message key"),
		New(&MockProvider{}),
		"test",
		[]HandlerFunc{
			func(ctx *Context) error { fmt.Println("::: GetUint() handler 1"); return nil },
			func(ctx *Context) error { fmt.Println("::: GetUint() handler 2"); return ctx.Ack() },
		},
	)
	ctx.Configure(
		With("123", uint(123)),
		With("0", uint(0)),
	)
	tests := []struct {
		name      string
		ctx       *Context
		key       string
		wantValue uint
		wantOk    bool
	}{
		{
			name:      "123",
			ctx:       ctx,
			key:       "123",
			wantValue: 123,
			wantOk:    true,
		},
		{
			name:      "0",
			ctx:       ctx,
			key:       "0",
			wantValue: 0,
			wantOk:    true,
		},
		{
			name:      "not set",
			ctx:       ctx,
			key:       "other",
			wantValue: 0,
			wantOk:    false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ctx := tt.ctx
			gotValue, gotOk := ctx.GetUint(tt.key)
			if !reflect.DeepEqual(gotValue, tt.wantValue) {
				t.Errorf("GetUint() gotValue = %v, want %v", gotValue, tt.wantValue)
			}
			if gotOk != tt.wantOk {
				t.Errorf("GetUint() gotOk = %v, want %v", gotOk, tt.wantOk)
			}
		})
	}
}

func TestContext_Handle(t *testing.T) {
	type args struct {
		topic   string
		msg     Message
		options []ContextOption
	}
	tests := []struct {
		name    string
		ctx     *Context
		args    args
		wantErr bool
	}{
		{
			name: "no err",
			ctx: NewContext(
				context.Background(),
				nil,
				func() *Service {
					service := New(&MockProvider{})
					service.Subscribe("test", func(ctx *Context) error { return nil })
					return service
				}(),
				"test",
				nil,
			),
			args: args{
				topic: "test",
				msg:   NewMockMessage("message body", "message key"),
				options: []ContextOption{
					With("origin", "ctx.Handle"),
				},
			},
			wantErr: false,
		},
		{
			name: "no topic handler error",
			ctx: NewContext(
				context.Background(),
				nil,
				New(&MockProvider{}),
				"test",
				nil,
			),
			args: args{
				topic: "test",
				msg:   NewMockMessage("message body", "message key"),
				options: []ContextOption{
					With("origin", "ctx.Handle"),
				},
			},
			wantErr: true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if err := tt.ctx.Handle(tt.args.topic, tt.args.msg, tt.args.options...); (err != nil) != tt.wantErr {
				t.Errorf("Handle() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}

func TestContext_Has(t *testing.T) {
	ctx := NewContext(
		context.Background(),
		NewMockMessage("message body", "message key"),
		New(&MockProvider{}),
		"test",
		[]HandlerFunc{
			func(ctx *Context) error { fmt.Println("::: Has() handler 1"); return nil },
			func(ctx *Context) error { fmt.Println("::: Has() handler 2"); return ctx.Ack() },
		},
	)
	ctx.Configure(
		With("one", 100),
		With("two", 200.00),
		With("three", "300"),
	)
	tests := []struct {
		name   string
		ctx    *Context
		key    string
		wantOk bool
	}{
		{
			name:   "one",
			ctx:    ctx,
			key:    "one",
			wantOk: true,
		},
		{
			name:   "two",
			ctx:    ctx,
			key:    "two",
			wantOk: true,
		},
		{
			name:   "three",
			ctx:    ctx,
			key:    "three",
			wantOk: true,
		},
		{
			name:   "four",
			ctx:    ctx,
			key:    "four",
			wantOk: false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ctx := tt.ctx
			gotOk := ctx.Has(tt.key)
			if gotOk != tt.wantOk {
				t.Errorf("Get() gotOk = %v, want %v", gotOk, tt.wantOk)
			}
		})
	}
}

func TestContext_Hook(t *testing.T) {
	tests := []struct {
		name       string
		ctx        *Context
		hook       *MockHook
		wantAcked  bool
		wantNacked bool
		wantErr    bool
	}{
		{
			name: "acked",
			ctx: NewContext(
				context.Background(),
				NewMockMessage("", ""),
				New(&MockProvider{}),
				"test",
				[]HandlerFunc{func(ctx *Context) error { return ctx.Ack() }},
			),
			hook:      &MockHook{},
			wantAcked: true,
		},
		{
			name: "nacked",
			ctx: NewContext(
				context.Background(),
				NewMockMessage("", ""),
				New(&MockProvider{}),
				"test",
				[]HandlerFunc{func(ctx *Context) error { return ctx.Nack() }},
			),
			hook:       &MockHook{},
			wantNacked: true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ctx := tt.ctx
			ctx.Hook(tt.hook)

			if err := ctx.Next(); (err != nil) != tt.wantErr {
				t.Errorf("Hook() error = %v, wantErr %v", err, tt.wantErr)
			}
			if tt.hook.acked != tt.wantAcked {
				t.Errorf("Hook() acked = %v, want %v", tt.hook.acked, tt.wantAcked)
			}
			if tt.hook.nacked != tt.wantNacked {
				t.Errorf("Hook() nacked = %v, want %v", tt.hook.nacked, tt.wantNacked)
			}
		})
	}
}

func TestContext_Key(t *testing.T) {
	tests := []struct {
		name string
		ctx  *Context
		want string
	}{
		{
			name: "no err",
			ctx: NewContext(
				context.Background(),
				NewMockMessage("", "some-key"),
				New(&MockProvider{}),
				"test",
				[]HandlerFunc{func(ctx *Context) error { return ctx.Ack() }},
			),
			want: "some-key",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ctx := tt.ctx
			if got := ctx.Key(); got != tt.want {
				t.Errorf("Key() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestContext_Nack(t *testing.T) {
	var debug bool
	type fields struct {
		parent   context.Context
		msg      *MockMessage
		s        *Service
		topic    string
		handlers []HandlerFunc
	}
	tests := []struct {
		name     string
		fields   fields
		validate func(*Context, fields, *testing.T)
		wantErr  bool
	}{
		{
			name: "nack the message",
			fields: fields{
				parent: context.Background(),
				msg:    NewMockMessage("message body", "message key"),
				s:      New(&MockProvider{}),
				topic:  "test",
				handlers: []HandlerFunc{
					func(ctx *Context) error {
						if debug {
							fmt.Println("::: Nack() handler 1")
						}
						return nil
					},
					func(ctx *Context) error {
						if debug {
							fmt.Println("::: Nack() handler 2")
						}
						return ctx.Nack()
					},
				},
			},
			validate: func(ctx *Context, f fields, t *testing.T) {
				if !f.msg.nacked {
					t.Errorf("Context.Nack() expected msg to have been nacked")
				}
			},
			wantErr: false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ctx := NewContext(
				tt.fields.parent,
				tt.fields.msg,
				tt.fields.s,
				tt.fields.topic,
				tt.fields.handlers,
			)
			err := ctx.Next()
			if (err != nil) != tt.wantErr {
				t.Errorf("Nack() error = %v, wantErr %v", err, tt.wantErr)
			}
			tt.validate(ctx, tt.fields, t)
		})
	}
}

func TestContext_Next(t *testing.T) {
	tests := []struct {
		name    string
		ctx     *Context
		wantErr bool
	}{
		{
			name: "no err",
			ctx: NewContext(
				context.Background(),
				NewMockMessage("", "some-key"),
				New(&MockProvider{}),
				"test",
				[]HandlerFunc{func(ctx *Context) error { return nil }},
			),
			wantErr: false,
		},
		{
			name: "has err",
			ctx: NewContext(
				context.Background(),
				NewMockMessage("", "some-key"),
				New(&MockProvider{}),
				"test",
				[]HandlerFunc{func(ctx *Context) error { return fmt.Errorf("some error") }},
			),
			wantErr: true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ctx := tt.ctx
			if err := ctx.Next(); (err != nil) != tt.wantErr {
				t.Errorf("Next() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}

func TestContext_Publish(t *testing.T) {
	ctx := NewContext(
		context.WithValue(context.Background(), "four", 400),
		NewMockMessage("message body", "message key"),
		New(&MockProvider{}),
		"test",
		nil,
	)
	{
		err := ctx.Publish("test", "this is a message", NewKey)
		if err != nil {
			t.Errorf("Publish() error = %v, wantErr %v", err, false)
		}
	}
}

func TestContext_RawString(t *testing.T) {
	ctx := NewContext(
		context.WithValue(context.Background(), "four", 400),
		NewMockMessage("message body", "message key"),
		New(&MockProvider{}),
		"test",
		nil,
	)
	{
		want := "message body"
		got := ctx.RawString()

		if got != want {
			t.Errorf("RawString() got = %v, want %v", got, want)
		}
	}
}

func TestContext_Topic(t *testing.T) {
	tests := []struct {
		name string
		ctx  *Context
		want string
	}{
		{
			name: "no err",
			ctx: NewContext(
				context.Background(),
				NewMockMessage("", "some-key"),
				New(&MockProvider{}),
				"some-topic",
				[]HandlerFunc{func(ctx *Context) error { return ctx.Ack() }},
			),
			want: "some-topic",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ctx := tt.ctx
			if got := ctx.Topic(); got != tt.want {
				t.Errorf("Topic() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestContext_Value(t *testing.T) {
	ctx := NewContext(
		context.WithValue(context.Background(), "four", 400),
		NewMockMessage("message body", "message key"),
		New(&MockProvider{}),
		"test",
		nil,
	)
	ctx.Configure(
		With("one", 100),
		With("two", 200.00),
		With("three", "300"),
	)
	tests := []struct {
		name      string
		ctx       *Context
		key       string
		wantValue any
	}{
		{
			name:      "one",
			ctx:       ctx,
			key:       "one",
			wantValue: 100,
		},
		{
			name:      "two",
			ctx:       ctx,
			key:       "two",
			wantValue: 200.00,
		},
		{
			name:      "three",
			ctx:       ctx,
			key:       "three",
			wantValue: "300",
		},
		{
			name:      "four",
			ctx:       ctx,
			key:       "four",
			wantValue: 400,
		},
		{
			name:      "five",
			ctx:       ctx,
			key:       "five",
			wantValue: nil,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ctx := tt.ctx
			gotValue := ctx.Value(tt.key)
			if !reflect.DeepEqual(gotValue, tt.wantValue) {
				t.Errorf("Get() gotValue = %v, want %v", gotValue, tt.wantValue)
			}
		})
	}
}

func TestFromContext(t *testing.T) {
	ctx := NewContext(
		context.WithValue(context.Background(), "four", 400),
		NewMockMessage("message body", "message key"),
		New(&MockProvider{}),
		"test",
		nil,
	)
	ctx.Configure(
		With("one", 100),
		With("two", 200.00),
		With("three", "300"),
	)
	{
		want := 100

		var got int

		err := FromContext(ctx, "one", &got)
		if err != nil {
			t.Errorf("FromContext() error = %v, wantErr %v", err, false)
		}
		if got != want {
			t.Errorf("FromContext() got = %v, want %v", got, want)
		}
	}
	{
		want := 0

		var got int

		err := FromContext(ctx, "one", got)
		if err == nil {
			t.Errorf("FromContext() error = %v, wantErr %v", err, true)
		}
		if got != want {
			t.Errorf("FromContext() got = %v, want %v", got, want)
		}
	}
}

func TestInvalidTargetError_Error(t *testing.T) {
	type fields struct {
		Func string
		Type reflect.Type
	}
	tests := []struct {
		name   string
		fields fields
		want   string
	}{
		{
			name: "default",
			fields: fields{
				Func: "TestInvalidTargetError_Error",
				Type: reflect.TypeOf(func(v string) *string { return &v }("")),
			},
			want: "pubsub: TestInvalidTargetError_Error(nil *string)",
		},
		{
			name: "nil value",
			fields: fields{
				Func: "TestInvalidTargetError_Error",
				Type: nil,
			},
			want: "pubsub: TestInvalidTargetError_Error(nil)",
		},
		{
			name: "non-pointer value",
			fields: fields{
				Func: "TestInvalidTargetError_Error",
				Type: reflect.TypeOf(func(v string) string { return v }("")),
			},
			want: "pubsub: TestInvalidTargetError_Error(non-pointer string)",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			e := &InvalidTargetError{
				Func: tt.fields.Func,
				Type: tt.fields.Type,
			}
			if got := e.Error(); got != tt.want {
				t.Errorf("Error() = %v, want %v", got, tt.want)
			}
		})
	}
}
