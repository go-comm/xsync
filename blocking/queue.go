package blocking

import (
	"container/list"
	"context"
	"sync"
)

type Queue interface {
	Offer(ctx context.Context, x interface{}) bool
	Take(ctx context.Context) interface{}
}

func NewBoundedQueue(size int) *BoundedQueue {
	return &BoundedQueue{c: make(chan interface{}, size)}
}

type BoundedQueue struct {
	c chan interface{}
}

func (q *BoundedQueue) Offer(ctx context.Context, x interface{}) bool {
	if ctx == nil || ctx == context.TODO() || ctx == context.Background() {
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

func (q *BoundedQueue) Take(ctx context.Context) interface{} {
	if ctx == nil || ctx == context.TODO() || ctx == context.Background() {
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

func NewUnBoundedQueue() *UnBoundedQueue {
	return &UnBoundedQueue{
		list: list.New(),
		c:    make(chan interface{}, 1024),
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

func (q *UnBoundedQueue) Take(ctx context.Context) interface{} {
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
		if ctx == nil || ctx == context.TODO() || ctx == context.Background() {
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
