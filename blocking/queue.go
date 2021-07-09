package blocking

import (
	"container/list"
	"context"
	"sync"
)

type Queue interface {
	Offer(ctx context.Context, x interface{}) bool
	Poll(ctx context.Context) interface{}
	Put(ctx context.Context, x interface{}) bool
	Take(ctx context.Context) interface{}
	Remove(ctx context.Context, x interface{}) bool
}

func NewBoundedQueue(capacity int) Queue {
	return &queue{
		capacity: capacity,
		list:     list.New(),
		in:       make(chan struct{}, 0),
		out:      make(chan struct{}, 0),
	}
}

func NewUnBoundedQueue() Queue {
	return &queue{
		capacity: 1 << 31,
		list:     list.New(),
		in:       make(chan struct{}, 0),
		out:      make(chan struct{}, 0),
	}
}

type queue struct {
	mutex    sync.RWMutex
	list     *list.List
	capacity int
	in       chan struct{}
	out      chan struct{}
}

func (q *queue) Offer(ctx context.Context, x interface{}) bool {
	return q.enqueue(ctx, x, false)
}

func (q *queue) Put(ctx context.Context, x interface{}) bool {
	return q.enqueue(ctx, x, true)
}

func (q *queue) enqueue(ctx context.Context, x interface{}, wait bool) bool {
	finished := false
LOOP:
	for {
		q.mutex.Lock()
		if q.list.Len() < q.capacity {
			q.list.PushBack(x)
			finished = true
		}
		q.mutex.Unlock()
		if finished || !wait {
			break LOOP
		}
		if wait {
			select {
			case <-ctx.Done():
				break LOOP
			case q.in <- struct{}{}:
			}
		}
	}

	if finished {
		select {
		case q.out <- struct{}{}:
		default:
		}
	}
	return finished
}

func (q *queue) Remove(ctx context.Context, x interface{}) bool {
	removed := false
	q.mutex.Lock()
	for e := q.list.Front(); e != nil; e = e.Next() {
		if Equal(e.Value, x) {
			removed = true
			q.list.Remove(e)
		}
	}
	q.mutex.Unlock()

	if removed {
		select {
		case <-q.in:
		default:
		}
	}

	return true
}

func (q *queue) Take(ctx context.Context) interface{} {
	return q.dequeue(ctx, true)
}

func (q *queue) Poll(ctx context.Context) interface{} {
	return q.dequeue(ctx, false)
}

func (q *queue) dequeue(ctx context.Context, wait bool) interface{} {
	var result interface{}
LOOP:
	for {
		q.mutex.Lock()
		e := q.list.Front()
		if e != nil {
			result = e.Value
			q.list.Remove(e)
		}
		q.mutex.Unlock()

		if result != nil || !wait {
			break LOOP
		}

		if result == nil && wait {
			select {
			case <-q.out:
			case <-ctx.Done():
				break LOOP
			}
		}
	}

	if result != nil {
		select {
		case <-q.in:
		default:
		}
	}

	return result
}
