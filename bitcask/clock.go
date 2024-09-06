package bitcask

import "sync/atomic"

type clock struct {
	v uint32
}

var clk = clock{}

func Tick() uint32 {
	return atomic.AddUint32(&clk.v, 1)
}
