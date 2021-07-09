package gopool

import (
	"time"

	"github.com/go-comm/xsync/blocking"
)

func NewWithCached(opts ...Option) GoPool {
	return New(0, maxCoreSize, 30*time.Second, blocking.NewBoundedQueue(1), opts...)
}

func NewWithFixed(n int, opts ...Option) GoPool {
	return New(n, n, 0, blocking.NewUnBoundedQueue(), opts...)
}

func NewWithSingle(opts ...Option) GoPool {
	return New(1, 1, 0, blocking.NewUnBoundedQueue(), opts...)
}

func NewScheduled(n int, opts ...Option) ScheduledPool {
	return &scheduledPool{
		GoPool: New(n, n, 0, blocking.NewDelayedQueue(16), opts...),
	}
}
