package main

import (
	"context"

	"github.com/sirupsen/logrus"

	"github.com/mertenvg/pubsub"
	"github.com/mertenvg/pubsub/middleware"
	"github.com/mertenvg/pubsub/plugins/retry"
	"github.com/mertenvg/pubsub/providers/kafka"
)

const (
	serviceName               = "pubsub"
	awsRegion                 = "us-east-1"
	retryEventTopic           = "pubsub-retry-events"
	deadLetterEventTopic      = "pubsub-dead-letter-events"
	maxRetryAttempts     uint = 5
)

func main() {
	var brokers []string
	var log logrus.FieldLogger

	kafkaProv, err := kafka.NewProvider(brokers, serviceName, awsRegion)
	if err != nil {
		// ...
	}

	// Create a new pubsub service
	pubsubService := pubsub.New(
		kafkaProv,
		pubsub.WithLog(log),
		pubsub.WithMiddlewares(
			middleware.Logrus(log),
			middleware.Recover(nil),
		),
		pubsub.WithPlugin(retry.NewPlugin(retryEventTopic, deadLetterEventTopic, maxRetryAttempts)),
	)

	// Subscribe for messages/events
	pubsubService.Subscribe("topic", func(ctx *pubsub.Context) error {
		// ...
		return nil
	})

	// Publish a message/event
	err = pubsubService.Publish(context.TODO(), "topic", "any", pubsub.NewKey)
	if err != nil {
		// ...
	}

	// ... a few lines later ...

	err = pubsubService.Stop()
	if err != nil {
		// ...
	}
}
