package valkey

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/sirupsen/logrus"
	vk "github.com/valkey-io/valkey-go"
)

type HandlerFunc func(msg *Message)

type Subscription struct {
	topic    string
	client   vk.Client
	handlers []HandlerFunc
	started  bool
	ctx      context.Context
	stop     context.CancelFunc
	log      logrus.FieldLogger
	// streams mode config (nil = pub/sub mode)
	stream *streamConfig
}

type streamConfig struct {
	group    string
	consumer string
	batch    int64
	block    time.Duration
}

func NewSubscription(topic string, client vk.Client, log logrus.FieldLogger, stream *streamConfig) *Subscription {
	ctx, cancel := context.WithCancel(context.Background())
	return &Subscription{
		topic:  topic,
		client: client,
		ctx:    ctx,
		stop:   cancel,
		log:    log.WithField("topic", topic),
		stream: stream,
	}
}

func (s *Subscription) Add(handler HandlerFunc) {
	s.log.Debug("add handler to subscription")
	s.handlers = append(s.handlers, handler)
	s.Start()
}

func (s *Subscription) Start() {
	s.log.Debug("start subscription")
	s.started = true
	if s.stream != nil {
		go s.listenStream()
	} else {
		go s.listenPubSub()
	}
}

func (s *Subscription) listenPubSub() {
	s.log.Debug("listen for pub/sub messages")
	err := s.client.Receive(s.ctx, s.client.B().Subscribe().Channel(s.topic).Build(), func(msg vk.PubSubMessage) {
		s.log.WithField("channel", msg.Channel).Debug("received pub/sub message")

		var envelope pubsubEnvelope
		if err := json.Unmarshal([]byte(msg.Message), &envelope); err != nil {
			s.log.WithError(err).Warn("cannot unmarshal pub/sub envelope - skipping message")
			return
		}

		m := &Message{
			key:  []byte(envelope.Key),
			data: envelope.Data,
		}

		for i, handler := range s.handlers {
			s.log.WithField("handler", i).Debug("handle message")
			handler(m)
		}
	})
	if err != nil && s.ctx.Err() == nil {
		s.log.WithError(err).Warn("pub/sub receive ended with error")
	}
}

func (s *Subscription) listenStream() {
	s.log.Debug("listen for stream messages")

	// ensure the consumer group exists
	err := s.client.Do(s.ctx,
		s.client.B().XgroupCreate().Key(s.topic).Group(s.stream.group).Id("0").Mkstream().Build(),
	).Error()
	if err != nil && !isGroupExistsError(err) {
		s.log.WithError(err).Error("cannot create consumer group")
		return
	}

	for {
		select {
		case <-s.ctx.Done():
			s.log.Debug("stop listening for stream messages")
			return
		default:
			result, err := s.client.Do(s.ctx,
				s.client.B().Xreadgroup().
					Group(s.stream.group, s.stream.consumer).
					Count(s.stream.batch).
					Block(s.stream.block.Milliseconds()).
					Streams().Key(s.topic).Id(">").
					Build(),
			).AsXRead()

			if err != nil {
				if s.ctx.Err() != nil {
					return
				}
				if vk.IsValkeyNil(err) {
					continue
				}
				s.log.WithError(err).Warn("cannot read stream messages - retrying")
				continue
			}

			entries, ok := result[s.topic]
			if !ok {
				continue
			}

			for _, entry := range entries {
				m := &Message{
					key:      []byte(entry.FieldValues["key"]),
					data:     []byte(entry.FieldValues["data"]),
					streamID: entry.ID,
					topic:    s.topic,
					group:    s.stream.group,
					client:   s.client,
				}

				for i, handler := range s.handlers {
					s.log.WithFields(logrus.Fields{
						"key":     entry.FieldValues["key"],
						"handler": i,
					}).Debug("handle stream message")
					handler(m)
				}
			}
		}
	}
}

func (s *Subscription) Stop() {
	s.log.Debug("stop subscription")
	s.stop()
	s.started = false
}

func isGroupExistsError(err error) bool {
	if err == nil {
		return false
	}
	// Valkey/Redis returns BUSYGROUP when the consumer group already exists
	vkErr, ok := err.(*vk.ValkeyError)
	if !ok {
		return false
	}
	return len(vkErr.Error()) > 9 && vkErr.Error()[:9] == "BUSYGROUP"
}

// pubsubEnvelope wraps key+data for the pub/sub mode since PUBLISH only accepts a single payload.
type pubsubEnvelope struct {
	Key  string `json:"key"`
	Data []byte `json:"data"`
}

func encodePubSubEnvelope(key, data []byte) (string, error) {
	bs, err := json.Marshal(pubsubEnvelope{
		Key:  string(key),
		Data: data,
	})
	if err != nil {
		return "", fmt.Errorf("valkey: encode pub/sub envelope: %w", err)
	}
	return string(bs), nil
}
