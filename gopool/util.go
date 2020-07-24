package gopool

import (
	"fmt"
	"log"
	"runtime"
	"sync"
)

var stackBytesPool = sync.Pool{
	New: func() interface{} {
		return make([]byte, 4096)
	},
}

func PrintStack(err interface{}) {
	buf := stackBytesPool.Get().([]byte)
	n := runtime.Stack(buf[:], false)
	log.Printf("gopool:  %+v\n%s", err, string(buf[:n]))
	stackBytesPool.Put(buf)
}

func WrappedError(err interface{}) error {
	e, ok := err.(error)
	if !ok {
		e = fmt.Errorf("%v", err)
	}
	return e
}
