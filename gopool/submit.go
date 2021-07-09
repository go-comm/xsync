package gopool

import (
	"context"
	"errors"
	"sync"
	"time"
)

type Future interface {
	Message

	Wait() bool
	WaitTimeout(d time.Duration) bool
	Result() (result interface{}, err error)
}

type future struct {
	mutex    sync.RWMutex
	callable Callable
	complete chan struct{}
	err      error
	result   interface{}
	Message
}

func (f *future) Cancel() {
	f.flowComplete()
}

func (f *future) setResult(result interface{}, err error) {
	f.mutex.Lock()
	f.err = err
	f.result = result
	f.mutex.Unlock()
}

func (f *future) Wait() bool {
	<-f.complete
	return true
}

func (f *future) WaitTimeout(d time.Duration) bool {
	t := time.NewTimer(d)
	select {
	case <-f.complete:
	case <-t.C:
		return false
	}
	if !t.Stop() {
		select {
		case <-t.C:
		default:
		}
	}
	return true
}

func (f *future) flowComplete() {
	select {
	case <-f.complete:
	default:
		close(f.complete)
	}
}

func (f *future) Result() (result interface{}, err error) {
	f.mutex.RLock()
	err = f.err
	result = f.result
	f.mutex.RUnlock()
	return
}

func (f *future) errorHandler(m Message, err interface{}) {
	f.setResult(WrappedError(err), nil)
	f.flowComplete()
	PrintStack(err)
}

func (f *future) rejectMessage(m Message) {
	f.setResult(errors.New("gopool: message reject"), nil)
	f.flowComplete()
}

func (f *future) handleMessage(m Message) {
	select {
	case <-f.Context().Done():
		f.setResult(nil, f.Context().Err())
		f.flowComplete()
		return
	default:
	}
	func() {
		defer func() {
			if err := recover(); err != nil {
				f.setResult(nil, WrappedError(err))
			}
			f.flowComplete()
		}()
		f.setResult(f.callable.Call())
	}()
}

type CallFunc func() (interface{}, error)

func (cf CallFunc) Call() (interface{}, error) {
	return cf()
}

type Callable interface {
	Call() (interface{}, error)
}

func (p *goPool) SubmitFunc(ctx context.Context, callfunc func() (interface{}, error)) Future {
	return p.Submit(ctx, CallFunc(callfunc))
}

func (p *goPool) Submit(ctx context.Context, callable Callable) Future {
	if callable == nil {
		panic("gopool: callable is nil")
	}
	f := &future{
		callable: callable,
		complete: make(chan struct{}),
	}
	f.Message = p.ObtainMessage(ctx, nil, nil,
		WithHandleMessage(f.handleMessage),
		WithErrorHandler(f.errorHandler),
		WithRejectMessage(f.rejectMessage),
	)
	p.Send(f)
	return f
}
