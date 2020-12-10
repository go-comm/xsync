package gopool

import (
	"fmt"
	"runtime"
	"sync"

	"github.com/go-comm/xsync/internal/trace"
)

var (
	ERR trace.Logger = trace.NOOPLogger{}
	INF trace.Logger = trace.NOOPLogger{}
	WRN trace.Logger = trace.NOOPLogger{}
	DBG trace.Logger = trace.NOOPLogger{}
)

var stackBytesPool = sync.Pool{
	New: func() interface{} {
		return make([]byte, 4096)
	},
}

func PrintStack(err interface{}) {
	buf := stackBytesPool.Get().([]byte)
	n := runtime.Stack(buf[:], false)
	DBG.Println("gopool:", err, string(buf[:n]))
	stackBytesPool.Put(buf)
}

func WrappedError(err interface{}) error {
	e, ok := err.(error)
	if !ok {
		e = fmt.Errorf("%v", err)
	}
	return e
}
