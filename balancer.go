package picodata

import (
	"fmt"
	"slices"
	"sync"

	"git.picodata.io/core/picodata-go/strats"

	"github.com/jackc/pgx/v5/pgxpool"
)

type balancerImpl struct {
	current  uint64
	mu       sync.RWMutex
	conns    []*pgxpool.Pool
	connMap  map[string]int
	nextFunc strats.BalanceStratFunc
}

func newBalancer(pool []*pgxpool.Pool) *balancerImpl {
	cm := make(map[string]int, len(pool))

	for i, conn := range pool {
		addr := fmt.Sprintf("%s:%d", conn.Config().ConnConfig.Host, conn.Config().ConnConfig.Port)
		cm[addr] = i
	}

	return &balancerImpl{
		conns:    pool,
		connMap:  cm,
		current:  0,
		nextFunc: strats.RoundRobinFunc,
	}
}

func (b *balancerImpl) SetBalanceStratFunc(nf strats.BalanceStratFunc) {
	b.nextFunc = nf
}

func (b *balancerImpl) NextConnection() *pgxpool.Pool {
	b.mu.RLock()
	defer b.mu.RUnlock()

	if len(b.conns) == 0 {
		return nil
	}
	index := b.nextFunc(&b.current, uint64(len(b.conns)))

	return b.conns[index]
}

func (b *balancerImpl) Conns() []*pgxpool.Pool {
	b.mu.RLock()
	defer b.mu.RUnlock()

	return b.conns
}

func (b *balancerImpl) ConnMap() map[string]*pgxpool.Pool {
	b.mu.RLock()
	defer b.mu.RUnlock()

	connMap := make(map[string]*pgxpool.Pool, len(b.connMap))
	for name, index := range b.connMap {
		connMap[name] = b.conns[index]
	}
	return connMap
}

func (b *balancerImpl) AddConn(name string, p *pgxpool.Pool) {
	b.mu.Lock()
	defer b.mu.Unlock()

	if idx, ok := b.connMap[name]; ok {
		b.conns[idx] = p
	} else {
		b.conns = append(b.conns, p)
		b.connMap[name] = len(b.conns) - 1
	}
}

func (b *balancerImpl) RemoveConn(name string) {
	b.mu.Lock()
	defer b.mu.Unlock()

	index, ok := b.connMap[name]
	if !ok {
		return
	}
	delete(b.connMap, name)
	b.conns = slices.Delete(b.conns, index, index+1)

	for name, idx := range b.connMap {
		if idx > index {
			b.connMap[name] = idx - 1
		}
	}
}
