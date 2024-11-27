package pubsub

import (
	"github.com/sirupsen/logrus"
)

// WithPlugin adds a plugin to the service
func WithPlugin(p Plugin) ServiceOption {
	return func(s *Service) {
		// add to plugins so it can be started and stopped
		s.plugins = append(s.plugins, p)
		// add the middleware so it follows the correct call sequence with other middleware
		s.middleware = append(s.middleware, p.Middleware())
	}
}

// WithMiddlewares adds one or more middlewares to the service that will be used for all incoming messages.
// Use Service.Subscribe to add middleware for a specific topic only.
func WithMiddlewares(mws ...HandlerFunc) ServiceOption {
	return func(s *Service) {
		s.middleware = append(s.middleware, mws...)
	}
}

// WithSubscriber adds a provider to subscribe to for events.
// This can be useful when migrating from one provider to another gradually.
func WithSubscriber(sub Subscriber) ServiceOption {
	return func(s *Service) {
		s.subs = append(s.subs, sub)
	}
}

func WithLog(log logrus.FieldLogger) ServiceOption {
	return func(s *Service) {
		s.log = log
	}
}

func WithPublishRetryConfig(conf *PubRetryConf) ServiceOption {
	return func(s *Service) {
		s.pubRetry = conf
	}
}

func WithWorkerLimit(max uint) ServiceOption {
	return func(s *Service) {
		if max < 1 {
			max = 1
		}
		s.workers = max
		oldQueue := s.queue
		s.queue = make(chan task, max)
		// close the old queue to force any other listeners on the channel to exit and start
		// listening to the new queue, and move the messages to the new queue.
		close(oldQueue)
		for msg := range oldQueue {
			s.queue <- msg
		}
	}
}

// WithPublishHook adds a publish hook to the pubsub.Service that is called whenever a new message is published
func WithPublishHook(ph PublishHook) ServiceOption {
	return func(s *Service) {
		if ph == nil {
			return
		}
		s.pubHooks = append(s.pubHooks, ph)
	}
}
