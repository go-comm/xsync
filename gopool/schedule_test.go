package gopool

import (
	"context"
	"fmt"
	"log"
	"os"
	"time"
)

func Example_Schedule() {
	ERR = log.New(os.Stdout, "", log.LstdFlags|log.LUTC)
	INF = log.New(os.Stdout, "", log.LstdFlags|log.LUTC)
	WRN = log.New(os.Stdout, "", log.LstdFlags|log.LUTC)
	DBG = log.New(os.Stdout, "", log.LstdFlags|log.LUTC)

	p := NewScheduled(2)
	start := time.Now().Unix()
	for i := 0; i < 10; i++ {
		n := i / 3
		p.Schedule(context.TODO(), func() {
			fmt.Println(n, time.Now().Unix()-start)
		}, time.Second*time.Duration(n))
	}

	time.Sleep(time.Second * 4)

	// Output:
	// 0 0
	// 0 0
	// 0 0
	// 1 1
	// 1 1
	// 1 1
	// 2 2
	// 2 2
	// 2 2
	// 3 3
}
