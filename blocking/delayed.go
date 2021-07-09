package blocking

import (
	"container/heap"
	"context"
	"sync"
	"time"

	"github.com/go-comm/xsync/internal/pqueue"
)

type Delayed interface {
	Remain() time.Duration
	Index() int
	SetIndex(i int)
}

type delayed struct {
	remain time.Duration
	index  int
}

func (d *delayed) Remain() time.Duration {
	return d.remain
}

func (d *delayed) Index() int {
	return d.index
}

func (d *delayed) SetIndex(i int) {
	d.index = i
}

func ObtainDelayed(remain time.Duration) Delayed {
	return &delayed{remain, -1}
}

type DelayedQueue interface {
	Queue
}

func NewDelayedQueue(capacity int) *delayedQueue {
	return &delayedQueue{
		pq:      pqueue.New(capacity),
		changed: make(chan struct{}, 0),
	}
}

type delayedQueue struct {
	mutex   sync.RWMutex
	pq      pqueue.PriorityQueue
	changed chan struct{}
}

func (q *delayedQueue) notifyDataChanged() {
	select {
	case q.changed <- struct{}{}:
	default:
	}
}

func (q *delayedQueue) Put(ctx context.Context, x interface{}) bool {
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
	if setter, ok := x.(interface {
		SetIndex(int)
	}); ok {
		setter.SetIndex(item.Index)
	}
	addToFront := item.Index == 0
	if addToFront {
	}
	return true
}

func (q *delayedQueue) Take(ctx context.Context) interface{} {
LOOP:
	for {
		now := time.Now().UnixNano()
		q.mutex.Lock()
		item, limit := q.pq.PeekAndShift(now)
		q.mutex.Unlock()
		if item != nil {
			q.notifyDataChanged()
			return item.Value
		}
		nctx, cancel := context.WithTimeout(ctx, time.Duration(limit))
		select {
		case <-nctx.Done():
			cancel()
			break LOOP
		case <-q.changed:
			cancel()
		}
	}
	return nil
}

func (q *delayedQueue) Offer(ctx context.Context, x interface{}) bool {
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
	if setter, ok := x.(interface {
		SetIndex(int)
	}); ok {
		setter.SetIndex(item.Index)
	}
	addToFront := item.Index == 0
	if addToFront {
		q.notifyDataChanged()
	}
	return true
}

func (q *delayedQueue) Poll(ctx context.Context) interface{} {
	now := time.Now().UnixNano()
	q.mutex.Lock()
	item, _ := q.pq.PeekAndShift(now)
	q.mutex.Unlock()
	if item != nil {
		q.notifyDataChanged()
		return item.Value
	}
	return nil
}

func (q *delayedQueue) Remove(ctx context.Context, x interface{}) bool {
	getter, ok := x.(interface {
		Index() int
	})
	if !ok {
		return false
	}
	return q.RemoveIndex(ctx, getter.Index())
}

func (q *delayedQueue) RemoveIndex(ctx context.Context, i int) bool {
	q.mutex.Lock()
	removeFromFront := i == 0
	heap.Remove(&q.pq, i)
	q.mutex.Unlock()
	if removeFromFront {
		q.notifyDataChanged()
	}
	return true
}
