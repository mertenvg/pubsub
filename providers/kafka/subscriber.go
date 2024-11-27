package kafka

import (
	"bytes"
	"context"

	"github.com/segmentio/kafka-go"
	"github.com/sirupsen/logrus"
)

type HandlerFunc func(e *Message)

type ErrorList []error

func (el ErrorList) Error() string {
	if len(el) == 0 {
		return ""
	}
	buf := bytes.Buffer{}
	buf.WriteString("[")
	for i, e := range el {
		if i > 0 {
			buf.WriteString("; ")
		}
		buf.WriteString(e.Error())
	}
	buf.WriteString("]")
	return buf.String()
}

type Subscription struct {
	topic    string
	reader   *kafka.Reader
	handlers []HandlerFunc
	started  bool
	ctx      context.Context
	stop     context.CancelFunc
	log      logrus.FieldLogger
}

// NewSubscription creates a new Subscription for the specified topic
func NewSubscription(topic string, reader *kafka.Reader, log logrus.FieldLogger) *Subscription {
	ctx, cancel := context.WithCancel(context.Background())

	return &Subscription{
		topic:  topic,
		reader: reader,
		ctx:    ctx,
		stop:   cancel,
		log:    log.WithField("topic", topic),
	}
}

// Add a handler for this topic
func (s *Subscription) Add(handler HandlerFunc) {
	s.log.Debug("add handler to subscription")
	s.handlers = append(s.handlers, handler)
	s.Start()
}

// Start listening for new messages
func (s *Subscription) Start() {
	s.log.Debug("start subscription")
	s.started = true
	go s.listen()
}

// listen is the method that waits for new messages.
// This method is intended to be run as a goroutine
func (s *Subscription) listen() {
	s.log.Debug("listen for topic messages")

	for {
		select {
		case <-s.ctx.Done():
			s.log.Debug("stop listening for topic messages")
			return
		default:
			s.log.Debug("wait for next message")
			msg, err := s.reader.ReadMessage(s.ctx)

			if err != nil {
				s.log.WithError(err).Warn("cannot read message from subscription kafka reader - skipping message")
				continue
			}

			if msg.Value == nil {
				s.log.WithField("key", string(msg.Key)).Warn("message value was empty from subscription kafka reader - skipping message")
				continue
			}

			s.log.WithField("handlers", len(s.handlers)).Debug("send message to handlers")

			// only process if there is no error and the message body is not empty, send it to all the registered handlers
			for i, handler := range s.handlers {
				fields := logrus.Fields{
					"key":     string(msg.Key),
					"handler": i,
				}

				s.log.WithFields(fields).Debug("handle message")

				handler(NewMessage(msg))
			}
		}
	}
}

// Stop this subscriber from listening for more messages
func (s *Subscription) Stop() {
	s.log.Debug("stop subscription")
	err := s.reader.Close()
	if err != nil {
		s.log.WithError(err).Warn("cannot close subscription kafka reader")
	}
	s.stop()
	s.started = false
}
