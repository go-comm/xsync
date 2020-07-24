package gopool

import (
	"context"
	"errors"
)

type Future struct {
	callable Callable
	result   chan interface{}
	done     chan error
	ctx      context.Context
	cancel   context.CancelFunc
}

func (f *Future) Cancel() {
	f.cancel()
}

func (f *Future) Done() <-chan error {
	return f.done
}

func (f *Future) Result() <-chan interface{} {
	return f.result
}

func (f *Future) errorHandler(m *Message, err interface{}) {
	PrintStack(err)
	select {
	case f.done <- WrappedError(err):
	default:
	}
	if f.result != nil {
		close(f.result)
	}
	if f.done != nil {
		close(f.done)
	}
}

func (f *Future) rejectMessage(m *Message) {
	select {
	case f.done <- errors.New("gopool: message reject"):
	default:
	}
	if f.result != nil {
		close(f.result)
	}
	if f.done != nil {
		close(f.done)
	}
}

func (f *Future) run() {
	select {
	case <-f.ctx.Done():
		if err := f.ctx.Err(); err != nil {
			select {
			case f.done <- err:
			default:
			}
		}
		return
	default:
	}

	result, err := f.callable.Call()
	if f.result != nil {
		f.result <- result
	}
	if f.done != nil {
		f.done <- err
	}
}

type CallFunc func() (interface{}, error)

func (cf CallFunc) Call() (interface{}, error) {
	return cf()
}

type Callable interface {
	Call() (interface{}, error)
}

func (p *GoPool) SubmitFunc(ctx context.Context, callfunc func() (interface{}, error)) *Future {
	return p.Submit(ctx, CallFunc(callfunc))
}

func (p *GoPool) Submit(ctx context.Context, callable Callable) *Future {
	if callable == nil {
		panic("gopool: callable is nil")
	}
	var cancel context.CancelFunc
	ctx, cancel = context.WithCancel(ctx)
	f := &Future{
		callable: callable,
		result:   make(chan interface{}, 1),
		done:     make(chan error, 1),
		ctx:      ctx,
		cancel:   cancel,
	}
	p.Go(ctx, f.run, WithErrorHandler(f.errorHandler), WithRejectMessage(f.rejectMessage))
	return f
}
