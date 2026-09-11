package strategies

import (
	"math/rand/v2"
)

var _ BalanceStrategy = (*randomStrategy)(nil)

type randomStrategy struct{}

// NewRandomStrategy creates a strategy that chooses the next node randomly.
func NewRandomStrategy() BalanceStrategy {
	return randomStrategy{}
}

func (randomStrategy) Next() uint64 {
	return rand.Uint64()
}

func (randomStrategy) Type() string {
	return "Random"
}
