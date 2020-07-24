package gopool

import (
	"container/list"
	"context"
	"fmt"
	"log"
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

func New(coreSize, maxCoreSize int, keepAliveTime time.Duration, queue blocking.Queue, opts ...Option) *GoPool {
	p := &GoPool{
		coreSize:      int32(coreSize),
		maxCoreSize:   int32(maxCoreSize),
		keepAliveTime: keepAliveTime,
		queue:         queue,
	}
	for _, opt := range opts {
		opt(&p.opts)
	}
	if coreSize < 0 || maxCoreSize <= 0 || maxCoreSize < coreSize || keepAliveTime < 0 {
		panic(fmt.Errorf("gopool: illegal argument, coreSize:%v maxCoreSize:%v keepAliveTime:%v", coreSize, maxCoreSize, keepAliveTime))
	}
	if p.queue == nil {
		p.queue = blocking.NewUnBoundedQueue()
	}
	if p.opts.errorHandler == nil {
		p.opts.errorHandler = DefaultErrorHandler
	}
	if p.opts.rejectMessage == nil {
		p.opts.rejectMessage = DefaultRejectMessage
	}
	p.workers = list.New()
	return p
}

type Options struct {
	handleMessage func(*Message)
	rejectMessage func(*Message)
	errorHandler  func(*Message, interface{})
}

type Option func(*Options)

func WithHandleMessage(h func(*Message)) Option {
	return func(opts *Options) {
		opts.handleMessage = h
	}
}

func WithRejectMessage(h func(*Message)) Option {
	return func(opts *Options) {
		opts.rejectMessage = h
	}
}

func WithErrorHandler(h func(*Message, interface{})) Option {
	return func(opts *Options) {
		opts.errorHandler = h
	}
}

func DefaultErrorHandler(m *Message, err interface{}) {
	PrintStack(err)
}

func DefaultRejectMessage(m *Message) {
	log.Printf("gopool: message reject")
}

type Message struct {
	Ctx      context.Context
	opts     Options
	Arg      interface{}
	Callback func()
}

func clearMessage(m *Message) {
	if m != nil {
		m.Ctx = nil
		m.Arg = nil
		m.Callback = nil
		m.opts.handleMessage = nil
		m.opts.rejectMessage = nil
		m.opts.errorHandler = nil
	}
}

type GoPool struct {
	state         int32
	coreSize      int32
	maxCoreSize   int32
	keepAliveTime time.Duration
	queue         blocking.Queue
	opts          Options
	workers       *list.List
	mutex         sync.RWMutex
}

func (p *GoPool) Go(ctx context.Context, fn func(), opts ...Option) {
	p.Send(&Message{Ctx: ctx, Callback: fn}, opts...)
}

func (p *GoPool) Exec(ctx context.Context, arg interface{}, opts ...Option) {
	p.Send(&Message{Ctx: ctx, Arg: arg}, opts...)
}

func (p *GoPool) Send(m *Message, opts ...Option) {
	for _, opt := range opts {
		opt(&m.opts)
	}
	if m.Ctx == nil {
		m.Ctx = context.TODO()
	}
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

func (p *GoPool) WorkerCount() int {
	state := atomic.LoadInt32(&p.state)
	return int(p.workerCount(state))
}

func (p *GoPool) KeepAliveTime() time.Duration {
	return p.keepAliveTime
}

func (p *GoPool) isRunning(state int32) bool {
	return state < stateShutdown
}

func (p *GoPool) addWorker(m *Message) bool {
	for {
		state := atomic.LoadInt32(&p.state)
		if p.workerCount(state) >= p.maxCoreSize {
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
	reject := m.opts.rejectMessage
	if reject == nil {
		reject = p.opts.rejectMessage
	}
	if reject != nil {
		reject(m)
	}
}

func (p *GoPool) handleError(m *Message, e interface{}) {
	h := m.opts.errorHandler
	if h == nil {
		h = p.opts.errorHandler
	}
	if h != nil {
		h(m, e)
	}
}

func (p *GoPool) shutdownAndWait(wait bool) {
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
		if wait {
			for i := len(ws) - 1; i >= 0; i-- {
				ws[i].wait()
			}
		}
	}
}

func (p *GoPool) Shutdown() {
	p.shutdownAndWait(false)
}

func (p *GoPool) ShutdownAndWait() {
	p.shutdownAndWait(true)
}
