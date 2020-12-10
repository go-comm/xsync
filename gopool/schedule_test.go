package gopool

import (
	"context"
	"log"
	"os"
	"testing"
	"time"
)

func Test_Schedule(t *testing.T) {

	ERR = log.New(os.Stdout, "", log.LstdFlags|log.LUTC)
	INF = log.New(os.Stdout, "", log.LstdFlags|log.LUTC)
	WRN = log.New(os.Stdout, "", log.LstdFlags|log.LUTC)
	DBG = log.New(os.Stdout, "", log.LstdFlags|log.LUTC)

	p := NewScheduled(1)

	t.Log("begin. ", time.Now())
	f := p.Schedule(context.TODO(), func() {
		t.Log("Hello ", time.Now())
	}, time.Second*4)

	_ = f

	// f.Cancel()

	time.Sleep(5 * time.Second)
	t.Log("end. ", time.Now())
}
