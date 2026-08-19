package picodata

import (
	"log/slog"

	"git.picodata.io/core/drivers/picodata-go/strategies"

	"github.com/jackc/pgx/v5/pgxpool"
)

// router picks which pool to use for the next query, according to a
// [strategies.BalanceStrategy]. It has no notion of topology: it only asks
// the [poolManager] for the current pool count and the pool at a given index.
type router struct {
	logger          *slog.Logger
	pools           *poolManager
	current         uint64
	balanceStrategy strategies.BalanceStrategy
}

type routerOption func(*router)

// withRouterLogger takes the root logger and bakes the package-specific tag inside.
func withRouterLogger(l *slog.Logger) routerOption {
	return func(r *router) {
		r.logger = l.With(slog.String("component", "router"))
	}
}

// withBalanceStrategy sets the strategy used to pick a pool among the routable ones.
func withBalanceStrategy(s strategies.BalanceStrategy) routerOption {
	return func(r *router) {
		r.balanceStrategy = s
	}
}

func newRouter(pools *poolManager, opts ...routerOption) *router {
	r := &router{
		logger:          slog.New(slog.DiscardHandler),
		pools:           pools,
		balanceStrategy: strategies.NewRoundRobinStrategy(),
	}

	for _, opt := range opts {
		opt(r)
	}

	return r
}

func (r *router) nextConnection() *pgxpool.Pool {
	conn := r.pools.pick(func(size int) uint64 {
		return r.balanceStrategy.Next(&r.current, uint64(size))
	})

	if conn == nil {
		r.logger.Warn("connection not found in a pool")
	}

	return conn
}
