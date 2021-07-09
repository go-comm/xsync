package gopool

import (
	"container/list"
	"context"
	"sync/atomic"
	"time"
)

const (
	stateWorkerNone = iota
	stateWorkerRunning
	stateWorkerShutdown
	stateWorkerTerminal
)

func newWorker(p *goPool) *worker {
	w := &worker{
		p:     p,
		state: stateWorkerNone,
		done:  make(chan struct{}),
	}
	w.ctx, w.cancel = context.WithCancel(p.Context())
	return w
}

type worker struct {
	p      *goPool
	elem   *list.Element
	state  int32
	ctx    context.Context
	cancel context.CancelFunc
	done   chan struct{}
}

func (w *worker) isRunning(state int32) bool {
	return state == stateWorkerRunning
}

func (w *worker) run(m Message) {
	p := w.p
	defer func() {
		atomic.StoreInt32(&w.state, stateWorkerTerminal)
		close(w.done)
		p.removeWorker(w)
	}()

	atomic.StoreInt32(&w.state, stateWorkerRunning)

	if m != nil {
		w.dispatchMessage(m)
	}

	var ctx context.Context
	var cancel context.CancelFunc
	var im interface{}
	var state int32
	var keepAliveTime time.Duration

Loop:
	for {
		state = atomic.LoadInt32(&w.state)
		if w.isRunning(state) {
			keepAliveTime = p.KeepAliveTime()
			ctx = w.ctx
			if keepAliveTime > 0 {
				ctx, cancel = context.WithTimeout(w.ctx, keepAliveTime)
			}
			im = p.queue.Take(ctx)
			if cancel != nil {
				cancel()
				cancel = nil
			}
		} else if state <= stateWorkerShutdown {
			im = p.queue.Poll(ctx)
		} else {
			break Loop
		}
		if im == nil {
			// check again
			// sometimes msg is nil because of shutdown worker
			im = p.queue.Poll(ctx)
			if im == nil {
				break Loop
			}
		}
		if m, ok := im.(Message); ok {
			w.dispatchMessage(m)
		}
	}
}

func (w *worker) dispatchMessage(m Message) {
	p := w.p
	defer func() {
		if err := recover(); err != nil {
			p.handleError(m, err)
		}
		p.ReleaseMessage(m)
	}()

	select {
	case <-m.Context().Done():
		if err := m.Context().Err(); err != nil {
			p.handleError(m, err)
		}
		return
	default:
	}

	callback := m.Callback()
	if callback == nil {
		h := m.Options().HandleMessage
		if h == nil {
			h = p.Options().HandleMessage
		}
		if h != nil {
			h(m)
		}
	} else {
		callback()
	}
}

func (w *worker) shutdown() {
	if atomic.LoadInt32(&w.state) < stateWorkerShutdown {
		atomic.StoreInt32(&w.state, stateWorkerShutdown)
		cancel := w.cancel
		if cancel != nil {
			cancel()
		}
	}
}

func (w *worker) wait() {
	if atomic.LoadInt32(&w.state) < stateWorkerTerminal {
		<-w.done
	}
}

func (w *worker) Release() {
	w.p = nil
	w.ctx = nil
	w.cancel = nil
	w.elem = nil
}
