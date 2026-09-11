package strategies

// BalanceStrategy is the interface for load balancing strategies.
type BalanceStrategy interface {
	// Next returns a selector for the next pool. The caller reduces it against
	// the pool set it indexes, so a strategy needs no idea of the pool size.
	Next() uint64
	// Type returns the strategy type for identification.
	Type() string
}
