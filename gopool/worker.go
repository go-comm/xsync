package gopool

import (
	"container/list"
	"context"
	"sync/atomic"
)

const (
	stateWorkerNone = iota
	stateWorkerRunning
	stateWorkerShutdown
	stateWorkerTerminal
)

func newWorker(p *GoPool) *worker {
	w := &worker{
		p:     p,
		state: stateWorkerNone,
		done:  make(chan struct{}),
	}
	w.ctx, w.cancel = context.WithCancel(context.Background())
	return w
}

type worker struct {
	p            *GoPool
	firstMessage *Message
	elem         *list.Element
	state        int32
	ctx          context.Context
	cancel       context.CancelFunc
	done         chan struct{}
}

func (w *worker) isRunning(state int32) bool {
	return state == stateWorkerRunning
}

func (w *worker) run() {
	var ctx context.Context
	var cancel context.CancelFunc
	var state int32
	p := w.p
	defer func() {
		atomic.StoreInt32(&w.state, stateWorkerTerminal)
		close(w.done)
		p.removeWorker(w)
	}()
	keepAliveTime := p.KeepAliveTime()
	atomic.StoreInt32(&w.state, stateWorkerRunning)

	if w.firstMessage != nil {
		m := w.firstMessage
		w.firstMessage = nil
		w.dispatchMessage(m)
	}

Loop:
	for {
		state = atomic.LoadInt32(&w.state)
		ctx = w.ctx
		var im interface{}
		if w.isRunning(state) {
			keepAliveTime = p.KeepAliveTime()
			if keepAliveTime > 0 {
				ctx, cancel = context.WithTimeout(ctx, keepAliveTime)
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

		if m, ok := im.(*Message); ok {
			w.dispatchMessage(m)
		}
	}
}

func (w *worker) dispatchMessage(m *Message) {
	p := w.p
	defer func() {
		if err := recover(); err != nil {
			p.handleError(m, err)
		}
		clearMessage(m)
	}()

	if m.Ctx != nil {
		select {
		case <-m.Ctx.Done():
			if err := m.Ctx.Err(); err != nil {
				p.handleError(m, err)
			}
			return
		default:
		}
	}

	callback := m.Callback
	if callback == nil {
		h := m.opts.handleMessage
		if h == nil {
			h = p.opts.handleMessage
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
