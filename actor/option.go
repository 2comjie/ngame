package actor

import "time"

type options struct {
	mailboxSize      int
	updateInterval   time.Duration
	lifecycleTimeout time.Duration
}

type Option func(*options)

func defaultOptions() options {
	return options{
		mailboxSize:      128,
		updateInterval:   time.Second,
		lifecycleTimeout: 5 * time.Second,
	}
}

func WithMailboxSize(size int) Option {
	return func(o *options) { o.mailboxSize = size }
}

func WithUpdateInterval(interval time.Duration) Option {
	return func(o *options) { o.updateInterval = interval }
}

func WithLifecycleTimeout(timeout time.Duration) Option {
	return func(o *options) { o.lifecycleTimeout = timeout }
}
