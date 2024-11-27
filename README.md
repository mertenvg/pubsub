# pubsub

## Getting started

Works like Gin but for events

```go
package main

import (
	"github.com/sirupsen/logrus"

	"gitlab.wheniwork.com/go/pubsub"
	"gitlab.wheniwork.com/go/pubsub/hooks"
	"gitlab.wheniwork.com/go/pubsub/middleware"
	"gitlab.wheniwork.com/go/pubsub/plugins/retry"
	"gitlab.wheniwork.com/go/pubsub/providers/kafka"
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
	err = pubsubService.Publish("topic", "any", pubsub.NewKey)
	if err != nil {
		// ...
	}

	// ... a few lines later ...

	err = pubsubService.Stop()
	if err != nil {
		// ...
	}
}

```

## Troubleshooting

### Why am I not receiving any events?

1. **Has the topic been created in Kafka?** <br />On our local development environments these are created automatically when a message is published to a topic that doesn't exist. <br />For any other env you'll need to have them added manually. https://wheniwork.atlassian.net/browse/DEV2-536.  
2. **Have you added publisher/consumer permissions in IAM?** <br />This MR is a good example of what's needed. https://gitlab.wheniwork.com/devops/terraform/resource/application-aws-iam-role/-/merge_requests/138/diffs<br /> Don't forget to add the various topics you're using.
3. **I've added everything but I'm still not getting messages through!** <br /> Make sure your configured topics and the topics created in Kafka match. A simple typo can really mess things up and become really hard to debug. **Important! For some reason the segmentio reader will fail reading for all topics even if only one of them is incorrect.** 
