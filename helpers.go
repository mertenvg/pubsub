package pubsub

import (
	"bytes"
	"context"
	"time"

	"github.com/sirupsen/logrus"
)

const (
	publishRetryBackoffMultiplier float64 = 2
	publishRetryBackoffMax                = time.Second
)

func defaultPublishRetryBackoff(last time.Duration) time.Duration {
	next := time.Duration(publishRetryBackoffMultiplier * float64(last))
	if next > publishRetryBackoffMax {
		return publishRetryBackoffMax
	}
	return next
}

// publish recursively attempts to write the given messages to kafka
func (s *Service) publishWithRetry(ctx context.Context, topic string, data []byte, key []byte, attempt uint, retryDelay time.Duration) error {
	fields := logrus.Fields{
		"topic":       topic,
		"data len":    len(data),
		"key":         string(key),
		"attempt":     attempt,
		"retry delay": retryDelay,
	}
	// attempt to write the messages to writer. if it fails, recursively retry up to the publishRetryLimit
	s.log.WithFields(fields).Debug("publish message")
	err := s.pub.Publish(ctx, topic, data, key)
	if err != nil {
		s.log.WithFields(fields).WithError(err).Debug("publish failed - try again, maybe")

		// check if we've reached the retry limit
		if attempt+1 >= s.pubRetry.Limit {
			s.log.WithFields(fields).Debug("retry limit reached - no more retries for you")
			return err
		}

		// wait a little before trying again
		s.log.WithFields(fields).Debug("waiting...")
		wait := make(chan struct{})
		go func(delay time.Duration) {
			time.Sleep(delay)
			close(wait)
		}(retryDelay)
		// respect the context
		select {
		case <-wait:
		case <-ctx.Done():
			return ctx.Err()
		}

		s.log.WithFields(fields).Debug("try again")
		// recurse with incremented attempt count and new back off duration
		return s.publishWithRetry(ctx, topic, data, key, attempt+1, s.pubRetry.Backoff(retryDelay))
	}

	s.log.WithFields(fields).Debug("publish ok")
	// everything worked fine, nothing more to see here
	return nil
}

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
