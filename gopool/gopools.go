package gopool

import "github.com/go-comm/xsync/blocking"

func NewWithCached(opts ...Option) *GoPool {
	return New(0, maxCoreSize, 30, blocking.NewBoundedQueue(1), opts...)
}

func NewWithFixed(n int, opts ...Option) *GoPool {
	return New(n, n, 0, blocking.NewUnBoundedQueue(), opts...)
}

func NewWithSingle(opts ...Option) *GoPool {
	return New(1, 1, 0, blocking.NewUnBoundedQueue(), opts...)
}
