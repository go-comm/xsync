package gopool

import (
	"log"
	"testing"
)

func Test_Go(t *testing.T) {
	p := NewWithSingle()

	for i := 0; i < 5; i++ {

		func(i int) {
			p.Go(func() {
				t.Logf("show %v", i)
			})
		}(i)

	}

	p.ShutdownAndWait()
}

func Test_Exec(t *testing.T) {
	p := NewWithSingle(WithHandleMessage(func(m *Message) {
		t.Log(m.Arg)
	}))

	for i := 0; i < 5; i++ {

		p.Exec(i)

	}

	p.ShutdownAndWait()
}

func Test_Cached_Exec(t *testing.T) {
	p := NewWithCached(WithHandleMessage(func(m *Message) {
		log.Println(m.Arg)
	}))

	for i := 0; i < 500; i++ {

		p.Exec(100000 + i)

	}
	p.ShutdownAndWait()
}

func Benchmark_Cached(b *testing.B) {

	for i := 0; i < b.N; i++ {
		p := NewWithCached()
		for j := 0; j < 100000; j++ {
			func(j int) {
				p.Go(func() {
					_ = j
				})
			}(j)
		}
	}
}
