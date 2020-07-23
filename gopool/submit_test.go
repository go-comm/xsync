package gopool

import (
	"context"
	"testing"
	"time"
)

func Test_Submit(t *testing.T) {

	p := NewWithCached()

	expect := 2

	f := p.SubmitFunc(context.TODO(), func() (interface{}, error) {
		time.Sleep(time.Millisecond * 100)
		// wrong := 0
		// _ = 1 / wrong
		return expect, nil
	})

	if err := <-f.Done(); err != nil {
		t.Fatal(err)
	}
	got := <-f.Result()

	if got != expect {
		t.Fatalf("expect %v, but got %v", expect, got)
	}
}
