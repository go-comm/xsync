package gopool

import (
	"context"
	"sync"
	"time"

	"github.com/go-comm/xsync/blocking"
)

type ScheduledFuture interface {
	Cancel()
	Wait() bool
	WaitTimeout(d time.Duration) bool
	Err() error
}

type ScheduledPool interface {
	Schedule(ctx context.Context, fn func(), delay time.Duration, opts ...Option) ScheduledFuture
	ScheduleAt(ctx context.Context, fn func(), delay time.Time, opts ...Option) ScheduledFuture
}

type scheduledPool struct {
	pool GoPool
}

type scheduledFuture struct {
	mutex    sync.RWMutex
	ctx      context.Context
	p        *scheduledPool
	err      error
	complete chan struct{}
	fn       func()
	m        *Message
}

func (f *scheduledFuture) Cancel() {
	if f.m != nil {
		f.mutex.Lock()
		if f.m != nil {
			f.p.remove(f.m)
			f.m = nil
		}
		f.mutex.Unlock()
	}
	f.flowComplete()
}

func (f *scheduledFuture) setErr(err error) {
	f.mutex.Lock()
	f.err = err
	f.mutex.Unlock()
}

func (f *scheduledFuture) Err() (err error) {
	f.mutex.RLock()
	err = f.err
	f.mutex.RUnlock()
	return
}

func (f *scheduledFuture) Wait() bool {
	<-f.complete
	return true
}

func (f *scheduledFuture) WaitTimeout(d time.Duration) bool {
	t := time.NewTimer(d)
	select {
	case <-f.complete:
	case <-t.C:
		return false
	}
	if !t.Stop() {
		<-t.C
	}
	return true
}

func (f *scheduledFuture) flowComplete() {
	select {
	case <-f.complete:
	default:
		close(f.complete)
	}
}

func (f *scheduledFuture) run() {
	select {
	case <-f.ctx.Done():
		f.setErr(f.ctx.Err())
		f.flowComplete()
		return
	default:
	}
	func() {
		defer func() {
			if err := recover(); err != nil {
				f.setErr(WrappedError(err))
			}
			f.flowComplete()
		}()
		f.fn()
	}()
}

func (p *scheduledPool) Schedule(ctx context.Context, fn func(), delay time.Duration, opts ...Option) ScheduledFuture {
	f := &scheduledFuture{
		ctx:      ctx,
		complete: make(chan struct{}),
		p:        p,
		fn:       fn,
	}
	m := &Message{Ctx: ctx, remain: delay, Callback: f.run}
	f.m = m
	p.send(m, opts...)
	return f
}

func (p *scheduledPool) ScheduleAt(ctx context.Context, fn func(), delay time.Time, opts ...Option) ScheduledFuture {
	return p.Schedule(ctx, fn, delay.Sub(time.Now()), opts...)
}

func (p *scheduledPool) send(m *Message, opts ...Option) {
	for _, opt := range opts {
		opt(&m.opts)
	}
	if m.Ctx == nil {
		m.Ctx = context.TODO()
	}
	if p.pool.IsShutdown() || !p.pool.Queue().Offer(context.TODO(), m) {
		p.pool.Reject(m)
		return
	}
	p.pool.EnsurePrestart()
}

func (p *scheduledPool) remove(m *Message) {
	queue, ok := p.pool.Queue().(*blocking.DelayedQueue)
	if !ok {
		return
	}
	queue.Remove(context.TODO(), m)
}
