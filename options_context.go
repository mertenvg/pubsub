package pubsub

func With(key string, value any) ContextOption {
	return func(ctx *Context) {
		ctx.Set(key, value)
	}
}
