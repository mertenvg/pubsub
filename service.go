package pubsub

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"sync"
	"time"

	"github.com/gofrs/uuid/v5"
	"github.com/sirupsen/logrus"
	"google.golang.org/protobuf/proto"
)

const (
	PublishRetryLimit uint = 7
	PublishRetryDelay      = time.Millisecond * 100
)

// NewKey is purposefully an empty string, if an empty string
// is provided for the key we should attempt to generate one
const NewKey = ""

type empty struct{}

type task struct {
	topic string
	msg   Message
}

type PubRetryConf struct {
	Limit   uint
	Delay   time.Duration
	Backoff func(last time.Duration) time.Duration
}

type HandlerChain []HandlerFunc

type Group struct {
	middleware HandlerChain
	parent     *Service
}

func (g *Group) Group(handlers ...HandlerFunc) *Group {
	return &Group{
		middleware: append(g.middleware, handlers...),
		parent:     g.parent,
	}
}

func (g *Group) Subscribe(topic string, handlers ...HandlerFunc) {
	g.parent.Subscribe(topic, append(g.middleware, handlers...)...)
}

type Service struct {
	pubRetry    *PubRetryConf
	middleware  []HandlerFunc
	plugins     []Plugin
	pub         Publisher
	pubHooks    []PublishHook
	subs        []Subscriber
	handlers    map[string][]HandlerChain
	started     bool
	log         logrus.FieldLogger
	wg          sync.WaitGroup
	workers     uint
	queue       chan task
	stopWorkers chan empty
}

// New creates a new Service
func New(pubsub Provider, options ...ServiceOption) *Service {
	s := &Service{
		pubRetry: &PubRetryConf{
			Limit:   PublishRetryLimit,
			Delay:   PublishRetryDelay,
			Backoff: defaultPublishRetryBackoff,
		},
		pub:      pubsub,
		subs:     []Subscriber{pubsub},
		handlers: make(map[string][]HandlerChain),
		log:      logrus.New(),
		workers:  10,
		queue:    make(chan task, 10),
	}

	// apply the configuration options
	s.Configure(options...)

	// do this after configuration just in case the logger is overridden
	s.log = s.log.WithField("scope", "pubsub")

	return s
}

// Configure applies the configuration options to the service
func (s *Service) Configure(options ...ServiceOption) {
	// apply options
	for _, o := range options {
		o(s)
	}
}

func (s *Service) PublishAsync(ctx context.Context, topic string, value any, key string) {
	go func(sv *Service) {
		err := sv.Publish(ctx, topic, value, key)
		if err != nil {
			sv.log.WithError(err).WithFields(logrus.Fields{
				"scope": "pubsub.PublishAsync",
				"topic": topic,
				"key":   key,
			}).Error("cannot publish message")
		}
	}(s)
}

// Publish a message
func (s *Service) Publish(ctx context.Context, topic string, value any, key string) error {
	log := s.log.WithFields(logrus.Fields{
		"topic": topic,
		"key":   key,
	})
	var data bytes.Buffer

	// check the value type and marshal it into bytes.Buffer
	switch v := value.(type) {
	case Marshaler:
		log.Debug("value is a pubsub.Marshaler - marshalling as message")
		bs, err := v.MarshalMessage()
		if err != nil {
			return err
		}
		data.Write(bs)
	case proto.Message:
		log.Debug("value is a proto.Message - marshalling as proto")
		bs, err := proto.Marshal(v)
		if err != nil {
			return err
		}
		data.Write(bs)
	case string:
		log.Debug("value is a string - no marshalling required")
		data.WriteString(v)
	default:
		log.Debug("fallback to default - marshalling as json")
		bs, err := json.Marshal(v)
		if err != nil {
			return err
		}
		data.Write(bs)
	}

	// if the key is empty generate one, the "|| key == NewKey" is redundant here but added for clarity
	if len(key) == 0 || key == NewKey {
		log.Debug("key is empty - generate a new one")
		u4, err := uuid.NewV4()
		if err != nil {
			return err
		}
		key = u4.String()
	}

	bs := data.Bytes()

	// notify hooks of a new message being published.
	for _, h := range s.pubHooks {
		h(topic, bs, key)
	}

	// publish the event with retry
	return s.publishWithRetry(ctx, topic, data.Bytes(), []byte(key), 0, s.pubRetry.Delay)
}

// Subscribe to a topic and send all events to the provided HandlerFunc after running it through option Middleware
func (s *Service) Subscribe(topic string, handlers ...HandlerFunc) {
	if len(handlers) == 0 {
		return
	}

	log := s.log.WithFields(logrus.Fields{
		"topic": topic,
	})

	if _, ok := s.handlers[topic]; !ok {
		// Create a ProviderHandlerFunc that wraps the HandlerFunc
		providerHandler := func(msg Message) {
			s.queue <- task{
				topic: topic,
				msg:   msg,
			}
		}

		log.Debug("add subscriptions	for each provider")
		// Add the subscription to all subs
		for _, r := range s.subs {
			r.Subscribe(topic, providerHandler)
		}
	} else {
		log.Debug("we already have a subscription for this topic, skip adding it to the providers")
	}

	// register the handlers, so we can use them later to handle the messages.
	// Topics can be subscribed to multiple times with different handler chains
	// NB: create a copy of the `handlers` chain before adding it to the map. Go reuses the underlying
	// memory address for each subsequent call to the function which changes previously stored `handlers`.
	s.handlers[topic] = append(s.handlers[topic], append([]HandlerFunc{}, handlers...))

	log.Debug("start pubsub service")
	// Start the service automatically when we have a subscription
	s.start()
}

// Handle passes a given message to the designated topic handlers.
func (s *Service) Handle(topic string, msg Message, options ...ContextOption) error {
	chains, ok := s.handlers[topic]
	if !ok {
		return fmt.Errorf("no handlers available for topic '%s'", topic)
	}

	var errs ErrorList

	for _, handlers := range chains {
		ctx, cancel := context.WithCancel(context.Background())
		err := NewContext(ctx, msg, s, topic, append(append([]HandlerFunc{}, s.middleware...), handlers...)).Configure(options...).Next()
		cancel()
		if err != nil {
			errs = append(errs, err)
		}
	}

	if len(errs) == 0 {
		return nil
	}

	return errs
}

// start the service.
// Note: The service will be started automatically when Subscribe is called.
func (s *Service) start() {
	if s.started {
		return
	}

	s.started = true
	s.wg.Add(1)
	s.stopWorkers = make(chan empty)

	s.log.Debug("start plugins")
	// iterate the plugins to start them
	for _, p := range s.plugins {
		p.Start(s)
	}

	s.log.Debugf("start %v workers", s.workers)
	for i := uint(0); i < s.workers; i++ {
		go work(s, i+1)
	}
}

// work starts up a worker for processing incoming messages
func work(s *Service, id uint) {
	for {
		select {
		case <-s.stopWorkers:
			s.log.Debugf("stopping worker %v", id)
			return
		case task := <-s.queue:
			err := s.Handle(task.topic, task.msg)
			if err != nil {
				fields := logrus.Fields{
					"topic":    task.topic,
					"data len": len(task.msg.Data()),
					"key":      string(task.msg.Key()),
				}
				s.log.WithFields(fields).WithError(err).Warn("got unresolved error while handling message")
			}
		}
	}
}

// Stop the service and all its subscribers and plugins.
func (s *Service) Stop() error {
	if !s.started {
		s.log.Debug("pubsub not started - skipping stop")
		return nil
	}

	var errs ErrorList

	s.log.Debug("stop all subscriptions")
	// stop all readers
	for _, r := range s.subs {
		err := r.Stop()
		if err != nil {
			errs = append(errs, err)
		}
	}

	s.log.Debug("stop all plugins")
	// iterate the plugins to stop them
	for _, p := range s.plugins {
		err := p.Stop()
		if err != nil {
			errs = append(errs, err)
		}
	}

	close(s.stopWorkers)

	s.started = false
	s.wg.Done()

	if len(errs) == 0 {
		return nil
	}

	return errs
}

// Wait for the service to stop
func (s *Service) Wait() {
	s.log.Debug("wait for pubsub to stop")
	s.wg.Wait()
}

// Group creates a middleware group for Subscriptions that need the same middleware
// that cannot be applied to the service as a whole.
func (s *Service) Group(handlers ...HandlerFunc) *Group {
	return &Group{
		middleware: handlers,
		parent:     s,
	}
}
