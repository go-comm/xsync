package gopool

import (
	"container/list"
	"context"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"github.com/go-comm/xsync/blocking"
)

const (
	bits         = 29
	capacityMask = (1 << bits) - 1
	maxCoreSize  = capacityMask
)

const (
	stateRunning  = 0 << bits
	stateShutdown = 1 << bits
	stateTerminal = 2 << bits
)

func NewWithCached(opts ...Option) *GoPool {
	return New(0, maxCoreSize, 30, blocking.NewBoundedQueue(16), opts...)
}

func NewWithFixed(n int, opts ...Option) *GoPool {
	return New(n, n, 0, blocking.NewUnBoundedQueue(), opts...)
}

func NewWithSingle(opts ...Option) *GoPool {
	return New(1, 1, 0, blocking.NewUnBoundedQueue(), opts...)
}

func New(coreSize, maxCoreSize int, keepAliveTime time.Duration, queue blocking.Queue, opts ...Option) *GoPool {
	p := &GoPool{
		coreSize:      int32(coreSize),
		maxCoreSize:   int32(maxCoreSize),
		keepAliveTime: keepAliveTime,
	}
	for _, opt := range opts {
		opt(p)
	}
	if coreSize < 0 || maxCoreSize <= 0 || maxCoreSize < coreSize || keepAliveTime < 0 {
		panic(fmt.Errorf("gopool: illegal argument, coreSize:%v maxCoreSize:%v keepAliveTime:%v", coreSize, maxCoreSize, keepAliveTime))
	}
	if p.queue == nil {
		p.queue = blocking.NewUnBoundedQueue()
	}
	p.workers = list.New()
	return p
}

type Option func(*GoPool)

func WithKeepAliveTime(keepAliveTime time.Duration) Option {
	return func(p *GoPool) {
		p.keepAliveTime = keepAliveTime
	}
}

func WithHandleMessage(h func(*Message)) Option {
	return func(p *GoPool) {
		p.handeMessage = h
	}
}

func WithRejectMessage(h func(*Message)) Option {
	return func(p *GoPool) {
		p.rejectMessage = h
	}
}

type Message struct {
	Callback func()
	Arg      interface{}
}

type GoPool struct {
	state         int32
	coreSize      int32
	maxCoreSize   int32
	keepAliveTime time.Duration
	queue         blocking.Queue
	handeMessage  func(*Message)
	rejectMessage func(*Message)
	workers       *list.List
	mutex         sync.RWMutex
}

func (p *GoPool) Go(fn func()) {
	p.Send(&Message{Callback: fn})
}

func (p *GoPool) Exec(arg interface{}) {
	p.Send(&Message{Arg: arg})
}

func (p *GoPool) Send(m *Message) {
	state := atomic.LoadInt32(&p.state)
	if p.workerCount(state) < p.coreSize {
		if p.addWorker(m) {
			return
		}
		state = atomic.LoadInt32(&p.state)
	}

	if p.isRunning(state) && p.offerToQueue(m) {
		state = atomic.LoadInt32(&p.state)
		if p.workerCount(state) == 0 {
			p.addWorker(nil)
		}
	} else if !p.addWorker(m) {
		p.reject(m)
	}

}

func (p *GoPool) workerCount(state int32) int32 {
	return state & capacityMask
}

func (p *GoPool) isRunning(state int32) bool {
	return state < stateShutdown
}

func (p *GoPool) addWorker(m *Message) bool {
	for {
		state := atomic.LoadInt32(&p.state)
		if p.workerCount(state) > p.maxCoreSize {
			return false
		}
		if atomic.CompareAndSwapInt32(&p.state, state, state+1) {
			w := newWorker(p)
			w.firstMessage = m
			p.mutex.Lock()
			w.elem = p.workers.PushBack(w)
			p.mutex.Unlock()
			go w.run()
			break
		}
	}
	return true
}

func (p *GoPool) removeWorker(w *worker) {
	for {
		state := atomic.LoadInt32(&p.state)
		if atomic.CompareAndSwapInt32(&p.state, state, state-1) {
			break
		}
	}
	p.mutex.Lock()
	p.workers.Remove(w.elem)
	p.mutex.Unlock()
	w.p = nil
	w.elem = nil
	w.cancel = nil

	state := atomic.LoadInt32(&p.state)
	if p.isRunning(state) && p.workerCount(state) < p.coreSize {
		p.addWorker(nil)
	} else if state >= stateShutdown && p.workerCount(state) == 0 {
		atomic.StoreInt32(&p.state, state|stateTerminal)
	}
}

func (p *GoPool) offerToQueue(m *Message) bool {
	return p.queue.Offer(context.TODO(), m)
}

func (p *GoPool) reject(m *Message) {
	if p.rejectMessage != nil {
		p.rejectMessage(m)
	}
}

func (p *GoPool) Shutdown() {
	var ws []*worker
	state := atomic.LoadInt32(&p.state)
	if p.isRunning(state) {
		atomic.StoreInt32(&p.state, state|stateShutdown)
		p.mutex.RLock()
		for e := p.workers.Front(); e != nil; e = e.Next() {
			w := e.Value.(*worker)
			if w != nil {
				ws = append(ws, w)
			}
		}
		p.mutex.RLock()
		for i := len(ws) - 1; i >= 0; i-- {
			ws[i].shutdown()
		}
	}

}

func (p *GoPool) ShutdownAndWait() {
	var ws []*worker
	state := atomic.LoadInt32(&p.state)
	if p.isRunning(state) {
		atomic.StoreInt32(&p.state, state|stateShutdown)
		p.mutex.RLock()
		for e := p.workers.Front(); e != nil; e = e.Next() {
			w := e.Value.(*worker)
			if w != nil {
				ws = append(ws, w)
			}
		}
		p.mutex.RLock()
		for i := len(ws) - 1; i >= 0; i-- {
			ws[i].shutdown()
		}
		for i := len(ws) - 1; i >= 0; i-- {
			ws[i].wait()
		}
	}
}
