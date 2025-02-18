package strats

import (
	"math/rand/v2"
	"sync/atomic"
)

type BalanceStratType int

const (
	RoundRobinStrat BalanceStratType = iota
	RandomStrat
)

type BalanceStratFunc func(current *uint64, poolSize uint64) uint64

var (
	RoundRobinFunc BalanceStratFunc = func(current *uint64, size uint64) uint64 {
		index := atomic.AddUint64(current, 1) - 1
		return index % size
	}

	RandomFunc BalanceStratFunc = func(current *uint64, size uint64) uint64 {
		prev := atomic.LoadUint64(current)
		newVal := rand.Uint64() % size
		atomic.StoreUint64(current, newVal)
		return prev
	}
)
