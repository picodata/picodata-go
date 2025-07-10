package strategies

// Balancer is the interface for load balancing strategies.
type BalanceStrategy interface {
	// Next returns the next index to use from the pool, given the current index pointer and pool size.
	Next(current *uint64, poolSize uint64) uint64
	// Type returns the strategy type for identification.
	Type() string
}
