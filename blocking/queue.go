package blocking

import (
	"container/list"
	"context"
	"sync"
	"time"
)

var noWait, _ = context.WithDeadline(context.Background(), time.Unix(0, 0))

func NoWait() context.Context {
	return noWait
}

type Queue interface {
	Offer(ctx context.Context, x interface{}) bool
	Poll(ctx context.Context) interface{}
}

func NewBoundedQueue(size int) *BoundedQueue {
	return &BoundedQueue{c: make(chan interface{}, size)}
}

type BoundedQueue struct {
	c chan interface{}
}

func (q *BoundedQueue) Offer(ctx context.Context, x interface{}) bool {
	if ctx == nil || ctx == NoWait() {
		select {
		case q.c <- x:
			return true
		default:
			return false
		}
	}

	select {
	case <-ctx.Done():
		return false
	case q.c <- x:
		return true
	}
}

func (q *BoundedQueue) Poll(ctx context.Context) interface{} {
	if ctx == nil || ctx == NoWait() {
		select {
		case x := <-q.c:
			return x
		default:
			return nil
		}
	}

	select {
	case <-ctx.Done():
		return nil
	case x := <-q.c:
		return x
	}
}

func NewUnBoundedQueue(bufSize ...int) *UnBoundedQueue {
	size := 1024
	if len(bufSize) > 0 && bufSize[0] > 0 {
		size = bufSize[0]
	}
	return &UnBoundedQueue{
		list: list.New(),
		c:    make(chan interface{}, size),
	}
}

type UnBoundedQueue struct {
	list  *list.List
	mutex sync.RWMutex
	c     chan interface{}
}

func (q *UnBoundedQueue) Offer(ctx context.Context, x interface{}) bool {
	select {
	case q.c <- x:
		return true
	default:
		q.mutex.Lock()
		q.list.PushBack(x)
		q.mutex.Unlock()
		return true
	}
}

func (q *UnBoundedQueue) Poll(ctx context.Context) interface{} {
	select {
	case x := <-q.c:
		return x
	default:
		break
	}

	var result interface{}
	q.mutex.Lock()
INSERT:
	for {
		e := q.list.Front()
		if e == nil {
			break
		}
		if result == nil {
			result = e.Value
			q.list.Remove(e)
			continue
		}
		select {
		case q.c <- e.Value:
			q.list.Remove(e)
		default:
			break INSERT
		}
	}
	q.mutex.Unlock()

	if result == nil {
		if ctx == nil || ctx == NoWait() {
			select {
			case x := <-q.c:
				return x
			default:
				return nil
			}
		}

		select {
		case <-ctx.Done():
		case x := <-q.c:
			return x
		}
	}
	return result
}
