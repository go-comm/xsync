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
		time.Sleep(time.Millisecond * 1000)
		// wrong := 0
		// _ = 1 / wrong
		return expect, nil
	})
	if !f.WaitTimeout(time.Second * 10) {
		t.Fatal("Time out")
	}
	got, err := f.Result()
	if err != nil {
		t.Fatal(err)
	}
	if got != expect {
		t.Fatalf("expect %v, but got %v", expect, got)
	}

}
