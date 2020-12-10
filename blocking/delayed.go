package blocking

import (
	"container/heap"
	"context"
	"sync"
	"time"

	"github.com/go-comm/xsync/internal/pqueue"
)

type DelayedIndexSetter interface {
	SetIndex(i int)
}

type DelayedIndexGetter interface {
	Index() int
}

type Delayed interface {
	Value() interface{}
	Remain() time.Duration
}

func NewDelayedQueue(capacity int) *DelayedQueue {
	return &DelayedQueue{
		pq:      pqueue.New(capacity),
		changed: make(chan struct{}, 0),
	}
}

type DelayedQueue struct {
	mutex   sync.RWMutex
	pq      pqueue.PriorityQueue
	changed chan struct{}
}

func (q *DelayedQueue) Put(ctx context.Context, x interface{}) bool {
	if x == nil {
		return false
	}
	delayed, ok := x.(Delayed)
	if !ok {
		return false
	}
	item := &pqueue.Item{
		Value:    delayed,
		Priority: time.Now().UnixNano() + int64(delayed.Remain()),
	}
	q.mutex.Lock()
	heap.Push(&q.pq, item)
	q.mutex.Unlock()
	if setter, ok := x.(DelayedIndexSetter); ok {
		setter.SetIndex(item.Index)
	}
	addToFront := item.Index == 0
	if addToFront {
		select {
		case q.changed <- struct{}{}:
		default:
		}
	}
	return true
}

func (q *DelayedQueue) Take(ctx context.Context) interface{} {
LOOP:
	for {
		now := time.Now().UnixNano()
		q.mutex.Lock()
		item, limit := q.pq.PeekAndShift(now)
		q.mutex.Unlock()
		if item != nil {
			select {
			case q.changed <- struct{}{}:
			default:
			}
			return item.Value
		}
		wait := time.NewTimer(time.Duration(limit) + time.Microsecond)
		select {
		case <-ctx.Done():
			if !wait.Stop() {
				<-wait.C
			}
			break LOOP
		case <-q.changed:
			if !wait.Stop() {
				<-wait.C
			}
		case <-wait.C:
		}
	}
	return nil
}

func (q *DelayedQueue) Offer(ctx context.Context, x interface{}) bool {
	if x == nil {
		return false
	}
	delayed, ok := x.(Delayed)
	if !ok {
		return false
	}
	item := &pqueue.Item{
		Value:    x,
		Priority: time.Now().UnixNano() + int64(delayed.Remain()),
	}
	q.mutex.Lock()
	heap.Push(&q.pq, item)
	q.mutex.Unlock()
	if setter, ok := x.(DelayedIndexSetter); ok {
		setter.SetIndex(item.Index)
	}
	addToFront := item.Index == 0
	if addToFront {
		select {
		case q.changed <- struct{}{}:
		default:
		}
	}
	return true
}

func (q *DelayedQueue) Poll(ctx context.Context) interface{} {
	now := time.Now().UnixNano()
	q.mutex.Lock()
	item, _ := q.pq.PeekAndShift(now)
	q.mutex.Unlock()
	if item != nil {
		select {
		case q.changed <- struct{}{}:
		default:
		}
		return item.Value
	}
	return nil
}

func (q *DelayedQueue) Remove(ctx context.Context, x interface{}) bool {
	getter, ok := x.(DelayedIndexGetter)
	if !ok {
		return false
	}
	return q.RemoveIndex(ctx, getter.Index())
}

func (q *DelayedQueue) RemoveIndex(ctx context.Context, i int) bool {
	q.mutex.Lock()
	removeFromFront := i == 0
	heap.Remove(&q.pq, i)
	q.mutex.Unlock()
	if removeFromFront {
		select {
		case q.changed <- struct{}{}:
		default:
		}
	}
	return true
}

func (q *DelayedQueue) DrainTo(ctx context.Context, s []Delayed) int {
	var size int
	if len(s) <= 0 {
		return 0
	}
	now := time.Now().UnixNano()
	q.mutex.Lock()
	for {
		if size >= len(s) {
			break
		}
		item, _ := q.pq.PeekAndShift(now)
		if item == nil {
			break
		}
		s[size], _ = item.Value.(Delayed)
		size++
	}
	q.mutex.Unlock()
	if size > 0 {
		select {
		case q.changed <- struct{}{}:
		default:
		}
	}
	return size
}
