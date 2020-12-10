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

func New(coreSize, maxCoreSize int, keepAliveTime time.Duration, queue blocking.Queue, opts ...Option) GoPool {
	p := &goPool{
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
	DBG.Println("gopool: message reject")
}

type Message struct {
	Ctx      context.Context
	remain   time.Duration
	index    int
	opts     Options
	Arg      interface{}
	Callback func()
}

func (m *Message) Value() interface{} {
	return m
}

func (m *Message) Index() int {
	return m.index
}

func (m *Message) SetIndex(i int) {
	m.index = i
}

func (m *Message) Remain() time.Duration {
	return m.remain
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

type GoPool interface {
	Go(ctx context.Context, fn func(), opts ...Option)

	Exec(ctx context.Context, arg interface{}, opts ...Option)

	Send(m *Message, opts ...Option)

	IsRunning() bool

	IsShutdown() bool

	WorkerCount() int

	Queue() blocking.Queue

	Reject(m *Message)

	EnsurePrestart()

	Shutdown()

	ShutdownAndWait()

	SubmitFunc(ctx context.Context, callfunc func() (interface{}, error)) Future

	Submit(ctx context.Context, callable Callable) Future
}

type goPool struct {
	state         int32
	coreSize      int32
	maxCoreSize   int32
	keepAliveTime time.Duration
	queue         blocking.Queue
	opts          Options
	workers       *list.List
	mutex         sync.RWMutex
}

func (p *goPool) Go(ctx context.Context, fn func(), opts ...Option) {
	p.Send(&Message{Ctx: ctx, Callback: fn}, opts...)
}

func (p *goPool) Exec(ctx context.Context, arg interface{}, opts ...Option) {
	p.Send(&Message{Ctx: ctx, Arg: arg}, opts...)
}

func (p *goPool) Send(m *Message, opts ...Option) {
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

	if p.isRunning(state) && p.queue.Offer(context.TODO(), m) {
		state = atomic.LoadInt32(&p.state)
		if p.workerCount(state) == 0 {
			p.addWorker(nil)
		}
	} else if !p.addWorker(m) {
		p.Reject(m)
	}

}

func (p *goPool) EnsurePrestart() {
	state := atomic.LoadInt32(&p.state)
	wc := p.workerCount(state)
	if wc == 0 {
		p.addWorker(nil)
	} else if wc < p.coreSize {
		p.addWorker(nil)
	}
}

func (p *goPool) workerCount(state int32) int32 {
	return state & capacityMask
}

func (p *goPool) WorkerCount() int {
	state := atomic.LoadInt32(&p.state)
	return int(p.workerCount(state))
}

func (p *goPool) KeepAliveTime() time.Duration {
	return p.keepAliveTime
}

func (p *goPool) isRunning(state int32) bool {
	return state < stateShutdown
}

func (p *goPool) IsRunning() bool {
	return p.isRunning(atomic.LoadInt32(&p.state))
}

func (p *goPool) IsShutdown() bool {
	return !p.isRunning(atomic.LoadInt32(&p.state))
}

func (p *goPool) Queue() blocking.Queue {
	return p.queue
}

func (p *goPool) addWorker(m *Message) bool {
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

func (p *goPool) removeWorker(w *worker) {
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

func (p *goPool) Reject(m *Message) {
	reject := m.opts.rejectMessage
	if reject == nil {
		reject = p.opts.rejectMessage
	}
	if reject != nil {
		reject(m)
	}
}

func (p *goPool) handleError(m *Message, e interface{}) {
	h := m.opts.errorHandler
	if h == nil {
		h = p.opts.errorHandler
	}
	if h != nil {
		h(m, e)
	}
}

func (p *goPool) shutdownAndWait(wait bool) {
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

func (p *goPool) Shutdown() {
	p.shutdownAndWait(false)
}

func (p *goPool) ShutdownAndWait() {
	p.shutdownAndWait(true)
}
