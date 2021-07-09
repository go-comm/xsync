package gopool

import (
	"context"
	"sync"
	"time"

	"github.com/go-comm/xsync/blocking"
)

type ScheduledFuture interface {
	Message
	blocking.Delayed

	Wait() bool
	WaitTimeout(d time.Duration) bool
	Err() error
}

type ScheduledPool interface {
	GoPool

	Schedule(ctx context.Context, fn func(), delay time.Duration, opts ...Option) ScheduledFuture
	ScheduleAt(ctx context.Context, fn func(), delay time.Time, opts ...Option) ScheduledFuture
}

type scheduledPool struct {
	GoPool
}

type scheduledFuture struct {
	mutex    sync.RWMutex
	p        *scheduledPool
	fn       func()
	err      error
	complete chan struct{}

	Message
	blocking.Delayed
}

func (f *scheduledFuture) Cancel() {
	f.Message.Cancel()
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

func (f *scheduledFuture) handleMessage(m Message) {
	select {
	case <-f.Context().Done():
		f.setErr(f.Context().Err())
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
		complete: make(chan struct{}),
		p:        p,
		fn:       fn,
	}
	f.Delayed = blocking.ObtainDelayed(delay)
	f.Message = p.GoPool.ObtainMessage(ctx, nil, nil, WithHandleMessage(f.handleMessage))
	p.send(f)
	return f
}

func (p *scheduledPool) ScheduleAt(ctx context.Context, fn func(), delay time.Time, opts ...Option) ScheduledFuture {
	return p.Schedule(ctx, fn, delay.Sub(time.Now()), opts...)
}

func (p *scheduledPool) send(m Message) {
	if p.GoPool.IsShutdown() || !p.GoPool.Queue().Offer(context.TODO(), m) {
		p.GoPool.Reject(m)
		return
	}
	p.GoPool.EnsurePrestart()
}
