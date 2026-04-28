# pubsub

A Go publish/subscribe library that works like [Gin](https://github.com/gin-gonic/gin) but for events. It features middleware chains, pluggable transport providers, and a familiar handler-based API.

## Install

```bash
go get github.com/mertenvg/pubsub
```

## Getting Started

```go
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
```

## Providers

Providers are swappable transport backends that implement the `pubsub.Provider` interface.

### Kafka

Uses [segmentio/kafka-go](https://github.com/segmentio/kafka-go) with optional AWS MSK IAM authentication.

```go
import "github.com/mertenvg/pubsub/providers/kafka"

provider, err := kafka.NewProvider(brokers, serviceName, awsRegion,
	kafka.WithLog(log),
	kafka.WithBatchSize(10),
	kafka.WithBatchTimeout(200*time.Millisecond),
)
```

Pass an empty string for `awsRegion` to connect without IAM authentication.

### Valkey

Uses the official [valkey-go](https://github.com/valkey-io/valkey-go) client. Supports two messaging mechanisms selectable via options:

**Pub/Sub mode** (default) -- fire-and-forget, no persistence or consumer groups:

```go
import "github.com/mertenvg/pubsub/providers/valkey"

provider, err := valkey.NewProvider("localhost:6379",
	valkey.WithLog(log),
)
```

**Streams mode** -- persistent messages with consumer groups and explicit Ack/Nack:

```go
provider, err := valkey.NewProvider("localhost:6379",
	valkey.WithStreams("my-group", "my-consumer"),
	valkey.WithStreamBatch(10),
	valkey.WithStreamBlock(2*time.Second),
)
```

Use `valkey.WithClientOption` to configure TLS, authentication, and other client settings.

### Memory

An in-process provider useful for testing. Messages are delivered synchronously.

```go
import "github.com/mertenvg/pubsub/providers/memory"

provider := memory.NewProvider(
	memory.WithAck(),  // optionally wait for Ack/Nack before returning from Publish
)
```

## Service Options

| Option | Description |
|---|---|
| `WithLog(log)` | Set the logger (logrus.FieldLogger) |
| `WithMiddlewares(mws...)` | Add global middleware for all subscriptions |
| `WithPlugin(p)` | Add a plugin (registers its middleware and lifecycle) |
| `WithSubscriber(sub)` | Add an additional subscriber (useful for provider migrations) |
| `WithWorkerLimit(n)` | Set the number of worker goroutines (default: 10) |
| `WithPublishRetryConfig(conf)` | Configure publish retry limit, delay, and backoff |
| `WithPublishHook(hook)` | Add a hook called on every publish |

## Middleware

Middleware are `HandlerFunc`s that run before your subscriber handler. Call `ctx.Next()` to continue the chain.

**Built-in middleware:**

- `middleware.Logrus(log)` -- logs each message received
- `middleware.Recover(fn)` -- recovers from panics in the handler chain

```go
pubsubService := pubsub.New(provider,
	pubsub.WithMiddlewares(
		middleware.Logrus(log),
		middleware.Recover(nil),
	),
)
```

### Middleware Groups

Use `Group` to apply middleware to a subset of subscriptions, similar to Gin's router groups:

```go
authorized := pubsubService.Group(authMiddleware)
authorized.Subscribe("private-topic", handler)
```

## Context

The `*pubsub.Context` passed to handlers provides:

- `ctx.Bind(into)` -- deserialize the message (supports protobuf, JSON, and custom `Unmarshaler`)
- `ctx.Raw()` / `ctx.RawString()` -- access raw message bytes
- `ctx.Key()` -- the message key
- `ctx.Topic()` -- the topic the message was received on
- `ctx.Set(key, value)` / `ctx.Get(key)` -- store and retrieve metadata through the handler chain
- `ctx.Publish(topic, msg, key)` -- publish a new message from within a handler
- `ctx.Ack()` / `ctx.Nack()` -- acknowledge or reject the message
- `ctx.Abort()` -- stop the handler chain without returning an error
- `ctx.Next()` -- advance to the next handler in the chain (used in middleware)

## Plugins

Plugins implement the `pubsub.Plugin` interface and participate in the service lifecycle.

**Built-in plugins:**

- `plugins/retry` -- retry failed messages with a dead-letter queue

```go
import "github.com/mertenvg/pubsub/plugins/retry"

pubsub.WithPlugin(retry.NewPlugin(retryTopic, deadLetterTopic, maxAttempts))
```

## Hooks

- `hooks.PrometheusPublish(metricsProvider)` -- a publish hook that records Prometheus metrics for published messages

```go
import "github.com/mertenvg/pubsub/hooks"

pubsub.WithPublishHook(hooks.PrometheusPublish(metricsProvider))
```

## License

[The Unlicense](LICENSE)
