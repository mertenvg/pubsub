package kafka

import (
	"context"
	"time"

	"github.com/segmentio/kafka-go"
	"github.com/sirupsen/logrus"

	"github.com/mertenvg/pubsub"
)

type Option func(p *Provider)

// WithDialer configures the pubsub.Service to use the provided dialer
func WithDialer(dialer *kafka.Dialer) Option {
	return func(p *Provider) {
		if dialer == nil {
			return
		}
		p.dialer = dialer
	}
}

func WithLog(log logrus.FieldLogger) Option {
	return func(p *Provider) {
		p.log = log
	}
}

func WithBatchTimeout(timeout time.Duration) Option {
	return func(p *Provider) {
		p.batchTimeout = timeout
	}
}

func WithBatchSize(size uint) Option {
	return func(p *Provider) {
		p.batchSize = size
	}
}

// Provider implements pubsub.Provider
type Provider struct {
	name             string
	brokers          []string
	dialer           *kafka.Dialer
	topicWriters     map[string]*kafka.Writer
	topicSubscribers map[string]*Subscription
	batchTimeout     time.Duration
	batchSize        uint
	log              logrus.FieldLogger
}

// NewProvider creates a new kafka.Provider
func NewProvider(brokers []string, serviceName string, awsRegion string, opts ...Option) (*Provider, error) {
	dialer := NewDialer()

	if awsRegion != "" {
		var err error
		dialer, err = NewSecureDialer(awsRegion)
		if err != nil {
			return nil, err
		}
	}

	p := &Provider{
		name:             serviceName,
		brokers:          brokers,
		dialer:           dialer,
		topicWriters:     make(map[string]*kafka.Writer),
		topicSubscribers: make(map[string]*Subscription),
		log:              logrus.New(),
		batchSize:        1,
		batchTimeout:     100 * time.Millisecond,
	}

	for _, o := range opts {
		o(p)
	}

	p.log = p.log.WithField("scope", "pubsub kafka provider")

	return p, nil
}

// Publish implements pubsub.Publisher
func (p *Provider) Publish(ctx context.Context, topic string, data []byte, key []byte) error {
	fields := logrus.Fields{
		"topic":    topic,
		"data len": len(data),
		"key":      string(key),
	}
	p.log.WithFields(fields).Debug("look for topic writer")
	writer, ok := p.topicWriters[topic]
	if !ok {
		p.log.WithFields(fields).Debug("no writer found for topic - add new writer")
		writer = kafka.NewWriter(kafka.WriterConfig{
			Brokers:      p.brokers,
			Topic:        topic,
			Dialer:       p.dialer,
			BatchTimeout: p.batchTimeout,
			BatchSize:    int(p.batchSize),
		})
		p.topicWriters[topic] = writer
	}
	p.log.WithFields(fields).Debug("write message")
	return writer.WriteMessages(context.Background(), kafka.Message{
		Key:   key,
		Value: data,
	})
}

// Subscribe implements pubsub.Subscriber
func (p *Provider) Subscribe(topic string, h pubsub.ProviderHandlerFunc) {
	fields := logrus.Fields{
		"topic": topic,
	}
	p.log.WithFields(fields).Debug("look for topic subscription")
	sub, ok := p.topicSubscribers[topic]
	if !ok {
		p.log.WithFields(fields).Debug("no subscription found for topic - add new subscription")
		reader := kafka.NewReader(kafka.ReaderConfig{
			Brokers:                p.brokers,
			GroupID:                p.name,
			Topic:                  topic,
			MinBytes:               5e3,
			MaxBytes:               1e6,
			MaxWait:                time.Second * 2,
			WatchPartitionChanges:  true,
			PartitionWatchInterval: time.Minute * 15,
			Dialer:                 p.dialer,
		})
		sub = NewSubscription(topic, reader, p.log)
		p.topicSubscribers[topic] = sub
	}
	sub.Add(func(msg *Message) {
		h(msg)
	})
}

// Stop implements pubsub.Subscriber
func (p *Provider) Stop() error {
	p.log.Debug("stop all subscriptions")
	for _, sub := range p.topicSubscribers {
		sub.Stop()
	}
	for _, pub := range p.topicWriters {
		_ = pub.Close()
	}
	return nil
}
