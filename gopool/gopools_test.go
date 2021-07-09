package gopool

import (
	"context"
	"sync/atomic"
	"testing"
	"time"
)

func Test_Single_Go(t *testing.T) {
	p := NewWithSingle()
	got := make(chan int)
	expect := 10
	p.Go(context.TODO(), func() {
		got <- expect
	})

	c := <-got
	if c != expect {
		t.Fatalf("expect %v, but got %v", expect, c)
	}
}

func calcAdd(t *testing.T, p GoPool, n int, counter *int32) {
	for i := 1; i <= n; i++ {
		p.Exec(context.TODO(), int32(i))
	}
	// t.Log("workCount", p.WorkerCount())
	p.ShutdownAndWait()
	// t.Log("workCount", p.WorkerCount())

	got := int(atomic.LoadInt32(counter))
	expect := (n + 1) * n / 2
	if got != expect {
		t.Fatalf("expect %v, but got %v", expect, got)
	}
}

func Test_Cached_Exec(t *testing.T) {
	var couter int32
	var n int = 5000
	p := NewWithCached(WithHandleMessage(func(m Message) {
		v := m.Arg().(int32)
		atomic.AddInt32(&couter, v)
		time.Sleep(time.Millisecond * 20)
	}), WithRejectMessage(func(m Message) {
		t.Log("reject", m.Arg())
	}))
	calcAdd(t, p, n, &couter)
}

func Test_Fixed_Exec(t *testing.T) {
	var couter int32
	var n int = 5000
	p := NewWithFixed(300, WithHandleMessage(func(m Message) {
		v := m.Arg().(int32)
		atomic.AddInt32(&couter, v)
		time.Sleep(time.Millisecond * 20)
	}), WithRejectMessage(func(m Message) {
		t.Log("reject", m.Arg())
	}))

	calcAdd(t, p, n, &couter)
}

func Test_Single_Exec(t *testing.T) {
	var couter int32
	var n int = 500
	p := NewWithSingle(WithHandleMessage(func(m Message) {
		v := m.Arg().(int32)
		atomic.AddInt32(&couter, v)
		time.Sleep(time.Millisecond * 20)
	}), WithRejectMessage(func(m Message) {
		t.Log("reject", m.Arg())
	}))

	calcAdd(t, p, n, &couter)
}
