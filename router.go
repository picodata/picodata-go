package picodata

import (
	"log/slog"

	"git.picodata.io/core/drivers/picodata-go/strategies"
	tpm "git.picodata.io/core/drivers/picodata-go/topology"

	"github.com/jackc/pgx/v5/pgxpool"
)

// router picks which pool serves the next query, according to a
// [strategies.BalanceStrategy].
//
// It is the driver's only topology listener: routing is meaningful only once
// the pools it names are open, and a second listener would give no ordering.
type router struct {
	logger          *slog.Logger
	pm              *poolManager
	balanceStrategy strategies.BalanceStrategy
}

type routerOption func(*router)

func withRouterLogger(l *slog.Logger) routerOption {
	return func(r *router) {
		r.logger = l.With(slog.String("component", "router"))
	}
}

func withBalanceStrategy(s strategies.BalanceStrategy) routerOption {
	return func(r *router) {
		r.balanceStrategy = s
	}
}

func newRouter(pm *poolManager, opts ...routerOption) *router {
	r := &router{
		logger:          slog.New(slog.DiscardHandler),
		pm:              pm,
		balanceStrategy: strategies.NewRoundRobinStrategy(),
	}

	for _, opt := range opts {
		opt(r)
	}

	return r
}

func (r *router) nextConnection() *pgxpool.Pool {
	conn := r.pm.pick(r.balanceStrategy.Next())

	if conn == nil {
		r.logger.Warn("connection not found in a pool")
	}

	return conn
}

// OnTopologyChanged implements [tpm.Listener].
func (r *router) OnTopologyChanged(snapshot *tpm.ClusterTopology) {
	pools := r.pm.reconcile(snapshot)

	r.logger.Debug("routing updated",
		slog.Uint64("version", snapshot.Version),
		slog.Int("pools", pools),
	)
}
