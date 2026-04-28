package valkey

import (
	"context"
	"fmt"
	"time"

	"github.com/sirupsen/logrus"
	vk "github.com/valkey-io/valkey-go"

	"github.com/mertenvg/pubsub"
)

type Option func(p *Provider)

// WithLog sets the logger for the provider.
func WithLog(log logrus.FieldLogger) Option {
	return func(p *Provider) {
		p.log = log
	}
}

// WithStreams enables streams mode with consumer group support.
// In streams mode, messages are persistent and support explicit Ack/Nack.
func WithStreams(group, consumer string) Option {
	return func(p *Provider) {
		p.stream = &streamConfig{
			group:    group,
			consumer: consumer,
			batch:    10,
			block:    2 * time.Second,
		}
	}
}

// WithStreamBatch sets the number of messages to read per XREADGROUP call.
// Only applies in streams mode.
func WithStreamBatch(n int64) Option {
	return func(p *Provider) {
		if p.stream != nil {
			p.stream.batch = n
		}
	}
}

// WithStreamBlock sets the block timeout for XREADGROUP.
// Only applies in streams mode.
func WithStreamBlock(d time.Duration) Option {
	return func(p *Provider) {
		if p.stream != nil {
			p.stream.block = d
		}
	}
}

// WithClientOption allows overriding valkey client options (TLS, auth, etc.).
func WithClientOption(fn func(*vk.ClientOption)) Option {
	return func(p *Provider) {
		p.clientOptionFns = append(p.clientOptionFns, fn)
	}
}

// Provider implements pubsub.Provider using Valkey as the transport.
// By default it uses Valkey Pub/Sub (fire-and-forget). Use WithStreams
// to enable persistent streams with consumer groups.
type Provider struct {
	client          vk.Client
	addr            string
	clientOptionFns []func(*vk.ClientOption)
	subscribers     map[string]*Subscription
	log             logrus.FieldLogger
	stream          *streamConfig
}

// NewProvider creates a new Valkey provider.
func NewProvider(addr string, opts ...Option) (*Provider, error) {
	p := &Provider{
		addr:        addr,
		subscribers: make(map[string]*Subscription),
		log:         logrus.New(),
	}

	for _, o := range opts {
		o(p)
	}

	clientOpt := vk.ClientOption{
		InitAddress: []string{addr},
	}
	for _, fn := range p.clientOptionFns {
		fn(&clientOpt)
	}

	client, err := vk.NewClient(clientOpt)
	if err != nil {
		return nil, fmt.Errorf("valkey: new client: %w", err)
	}

	p.client = client
	p.log = p.log.WithField("scope", "pubsub valkey provider")

	return p, nil
}

// Publish implements pubsub.Publisher
func (p *Provider) Publish(ctx context.Context, topic string, data []byte, key []byte) error {
	fields := logrus.Fields{
		"topic":    topic,
		"data len": len(data),
		"key":      string(key),
	}

	if p.stream != nil {
		p.log.WithFields(fields).Debug("publish to stream")
		return p.client.Do(ctx,
			p.client.B().Xadd().Key(topic).Id("*").FieldValue().
				FieldValue("key", string(key)).
				FieldValue("data", string(data)).
				Build(),
		).Error()
	}

	p.log.WithFields(fields).Debug("publish to channel")
	envelope, err := encodePubSubEnvelope(key, data)
	if err != nil {
		return err
	}
	return p.client.Do(ctx,
		p.client.B().Publish().Channel(topic).Message(envelope).Build(),
	).Error()
}

// Subscribe implements pubsub.Subscriber
func (p *Provider) Subscribe(topic string, h pubsub.ProviderHandlerFunc) {
	fields := logrus.Fields{
		"topic": topic,
	}
	p.log.WithFields(fields).Debug("look for topic subscription")
	sub, ok := p.subscribers[topic]
	if !ok {
		p.log.WithFields(fields).Debug("no subscription found for topic - add new subscription")
		sub = NewSubscription(topic, p.client, p.log, p.stream)
		p.subscribers[topic] = sub
	}
	sub.Add(func(msg *Message) {
		h(msg)
	})
}

// Stop implements pubsub.Subscriber
func (p *Provider) Stop() error {
	p.log.Debug("stop all subscriptions")
	for _, sub := range p.subscribers {
		sub.Stop()
	}
	p.client.Close()
	return nil
}
