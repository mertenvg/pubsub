package hooks

import (
	"github.com/mertenvg/pubsub"
	"github.com/prometheus/client_golang/prometheus"
)

type MetricsProvider interface {
	PublishWithLabels(labels prometheus.Labels)
}

func PrometheusPublish(mp MetricsProvider) pubsub.PublishHook {
	return func(topic string, data []byte, key string) {
		labels := prometheus.Labels{
			"topic": topic,
		}
		mp.PublishWithLabels(labels)
	}
}
