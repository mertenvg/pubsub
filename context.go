package pubsub

import (
	"context"
	"encoding/json"
	"fmt"
	"reflect"
	"sync"
	"sync/atomic"

	"google.golang.org/protobuf/proto"
)

type ContextOption func(ctx *Context)

// Hook into ack or nack calls
type Hook interface {
	Ack(ctx *Context)
	Nack(ctx *Context)
}

// An InvalidTargetError describes an invalid argument passed to [FromContext] or [Bind].
// (The `into` argument must be a non-nil pointer.)
type InvalidTargetError struct {
	Func string
	Type reflect.Type
}

func (e *InvalidTargetError) Error() string {
	if e.Type == nil {
		return fmt.Sprintf("pubsub: %s(nil)", e.Func)
	}
	if e.Type.Kind() != reflect.Pointer {
		return fmt.Sprintf("pubsub: %s(non-pointer %s)", e.Func, e.Type)
	}
	return fmt.Sprintf("pubsub: %s(nil %s)", e.Func, e.Type)
}

// Context is the pubsub.Context and is composed with context.Context
type Context struct {
	context.Context

	topic    string
	s        *Service
	handlers []HandlerFunc
	index    int
	mu       sync.RWMutex
	meta     map[string]any
	msg      Message
	hooks    []Hook
	err      error
	aborted  atomic.Bool
}

// NewContext creates a new concurrency safe Context to store any value along with a key for later retrieval.
// Note: Consider using a sync.Pool for Context's as an optimisation.
func NewContext(parent context.Context, msg Message, s *Service, topic string, handlers []HandlerFunc) *Context {
	ctx := &Context{
		Context:  parent,
		topic:    topic,
		s:        s,
		handlers: handlers,
		index:    -1,
		mu:       sync.RWMutex{},
		meta:     map[string]any{},
		msg:      msg,
	}
	s.log.Debugf("ctx('%p'): creating new context for topic '%v' with handlers '%v'", ctx, topic, handlers)
	return ctx
}

// Set a value in the Context
func (ctx *Context) Set(key string, value any) {
	ctx.mu.Lock()
	ctx.meta[key] = value
	ctx.mu.Unlock()
}

// Get a value from the Context
func (ctx *Context) Get(key string) (value any, ok bool) {
	ctx.mu.RLock()
	value, ok = ctx.meta[key]
	ctx.mu.RUnlock()
	return
}

// GetBool gets a value from the context as a bool type
func (ctx *Context) GetBool(key string) (value bool, ok bool) {
	var v any
	if v, ok = ctx.Get(key); ok {
		value, ok = v.(bool)
	}
	return
}

// GetString gets a value from the context as a string type
func (ctx *Context) GetString(key string) (value string, ok bool) {
	var v any
	if v, ok = ctx.Get(key); ok {
		value, ok = v.(string)
	}
	return
}

// GetUint gets a value from the context as a uint type
func (ctx *Context) GetUint(key string) (value uint, ok bool) {
	var v any
	if v, ok = ctx.Get(key); ok {
		value, ok = v.(uint)
	}
	return
}

// GetInt gets a value from the context as a int type
func (ctx *Context) GetInt(key string) (value int, ok bool) {
	var v any
	if v, ok = ctx.Get(key); ok {
		value, ok = v.(int)
	}
	return
}

// GetFloat gets a value from the context as a float64 type
func (ctx *Context) GetFloat(key string) (value float64, ok bool) {
	var v any
	if v, ok = ctx.Get(key); ok {
		value, ok = v.(float64)
	}
	return
}

// Has a key in the Context
func (ctx *Context) Has(key string) (ok bool) {
	ctx.mu.RLock()
	_, ok = ctx.meta[key]
	ctx.mu.RUnlock()
	return
}

func (ctx *Context) Topic() string {
	return ctx.topic
}

// Key return the message key as a byte slice
func (ctx *Context) Key() string {
	return string(ctx.msg.Key())
}

// Raw message data as a byte slice
func (ctx *Context) Raw() []byte {
	return ctx.msg.Data()
}

// RawString is like Raw except it returns a string
func (ctx *Context) RawString() string {
	return string(ctx.Raw())
}

// Bind the message into the given value
func (ctx *Context) Bind(into any) error {
	// check that `into` is a pointer and is not nil
	rv := reflect.ValueOf(into)
	if rv.Kind() != reflect.Pointer || rv.IsNil() {
		return &InvalidTargetError{Func: "Context.Bind", Type: reflect.TypeOf(into)}
	}
	// depending on the provided `into` type let's try to Unmarshal the message data accordingly,
	// defaulting to json if nothing matches.
	switch target := into.(type) {
	case Unmarshaler:
		return target.UnmarshalMessage(ctx.Raw())
	case proto.Message:
		return proto.Unmarshal(ctx.Raw(), target)
	case *string:
		*target = ctx.RawString()
		return nil
	default:
		return json.Unmarshal(ctx.Raw(), target)
	}
}

// Ack the message
func (ctx *Context) Ack() error {
	for _, h := range ctx.hooks {
		h.Ack(ctx)
	}
	return ctx.msg.Ack()
}

// Nack the message
func (ctx *Context) Nack() error {
	for _, h := range ctx.hooks {
		h.Nack(ctx)
	}
	return ctx.msg.Nack()
}

// Publish a new message to the same pubsub service
func (ctx *Context) Publish(topic string, msg any, key string) error {
	return ctx.s.Publish(ctx, topic, msg, key)
}

// Handle a message as if it came from the provider
func (ctx *Context) Handle(topic string, msg Message, options ...ContextOption) error {
	return ctx.s.Handle(topic, msg, options...)
}

// Hook adds a hook into the context to be notified of message Ack's and Nack's
func (ctx *Context) Hook(h Hook) {
	ctx.hooks = append(ctx.hooks, h)
}

// Next calls the next handler in the chain. Multiple calls to Next within the chain will simply advance the chain
// until there are no more handlers left. If Next is skipped or forgotten in one of the handlers then it will
// automatically advance to the next one when the current handler finishes without any errors.
func (ctx *Context) Next() error {
	// Listen to the context for cancellations, timeouts, or deadlines
	go func() {
		<-ctx.Done()
		ctx.Abort()
	}()

	ctx.s.log.Debugf("ctx('%p'): next called at handler '%v' of '%v'", ctx, ctx.index+1, len(ctx.handlers))

	// The first handler will have been called already to get to this point
	ctx.index++

	// iterate while the index is less than the number of handlers we have, and the context hasn't been aborted
	for ctx.index < len(ctx.handlers) && !ctx.aborted.Load() {
		h := ctx.handlers[ctx.index]

		ctx.s.log.Debugf("ctx('%p'): running handler '%v' of '%v' ('%v')", ctx, ctx.index+1, len(ctx.handlers), h)

		// if an error is returned stop iterating the chain and return the error
		err := h(ctx)
		if err != nil {
			ctx.s.log.Debugf("ctx('%p'): handler '%v' of '%v' ('%v') returned with an error: %v", ctx, ctx.index+1, len(ctx.handlers), h, err)
			return err
		}

		// move the index so we advance down the chain automatically
		ctx.index++
	}

	if ctx.aborted.Load() {
		ctx.s.log.Debugf("ctx('%p'): context was aborted at handler '%v' of '%v'", ctx, ctx.index+1, len(ctx.handlers))
	}

	return ctx.Err()
}

// Abort calling the remaining handlers without returning an error
func (ctx *Context) Abort() {
	// abort atomically to make the operation concurrency safe
	ctx.aborted.Store(true)
}

// Configure the Context with option functions. Returns the same Context for call chaining
func (ctx *Context) Configure(options ...ContextOption) *Context {
	for _, o := range options {
		o(ctx)
	}
	return ctx
}

// Err overrides Err from parent context.Context
func (ctx *Context) Err() error {
	if ctx.err != nil {
		return ctx.err
	}
	return ctx.Context.Err()
}

// Value overrides Value from parent context.Context. For *pubsub.Context use ctx.Get*(key string)
func (ctx *Context) Value(key any) any {
	if keyAsString, ok := key.(string); ok {
		if value, found := ctx.Get(keyAsString); found {
			return value
		}
	}
	return ctx.Context.Value(key)
}

// FromContext retrieves a value for the provided key and resolves it into the provided pointer. This function works
// for any context.Context including *pubsub.Context
func FromContext[T any](ctx context.Context, key any, into T) error {
	value := ctx.Value(key)
	rv := reflect.ValueOf(into)
	if rv.Kind() != reflect.Pointer {
		return &InvalidTargetError{Func: "FromContext", Type: reflect.TypeOf(value)}
	}
	if value != nil {
		reflect.ValueOf(into).Elem().Set(reflect.ValueOf(value))
	}
	return nil
}
