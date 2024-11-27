package plugin

import (
	"github.com/mertenvg/pubsub"
)

type Plugin struct {
}

// NewPlugin creates a new logrus.Plugin
func NewPlugin() *Plugin {
	return &Plugin{}
}

// Middleware implements pubsub.Plugin
func (p *Plugin) Middleware() pubsub.HandlerFunc {
	return func(ctx *pubsub.Context) error {
		return ctx.Next()
	}
}

// Start implements pubsub.Plugin
func (p *Plugin) Start(s *pubsub.Service) {
	_ = s
}

// Stop implements pubsub.Plugin
func (p *Plugin) Stop() error {
	return nil
}
