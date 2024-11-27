package pubsub

import (
	"context"
	"testing"
)

func TestWith(t *testing.T) {
	type args struct {
		key   string
		value any
	}
	tests := []struct {
		name     string
		args     args
		validate func(*Context, *testing.T)
	}{
		{
			name: "add value to context",
			args: args{
				key:   "value-key",
				value: "value-value",
			},
			validate: func(ctx *Context, t *testing.T) {
				got, ok := ctx.GetString("value-key")
				if !ok {
					t.Errorf("With() expected key %v to be set on the context", "value-key")
				}
				if got != "value-value" {
					t.Errorf("With() got `%s`, expected `%s`", got, "value-value")
				}
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ctx := NewContext(context.Background(), nil, New(&MockProvider{}), "topic", nil)
			With(tt.args.key, tt.args.value)(ctx)
			tt.validate(ctx, t)
		})
	}
}
