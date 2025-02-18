package picodata

import (
	"context"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"
)

const (
	peerAddressTable = "_pico_peer_address"
	addressColumn    = "address"
	connTypeColumn   = "connection_type"
	pollPeriod       = 500 * time.Millisecond
)

type Pool struct {
	ctx       context.Context
	cf        context.CancelFunc
	masterCfg *pgxpool.Config
	balancer  *balancerImpl
	manager   poller
}

// Create pool with postgresql connection string
func New(ctx context.Context, connString string, opts ...PoolOption) (*Pool, error) {
	cfg, err := pgxpool.ParseConfig(connString)
	if err != nil {
		return nil, err
	}

	return NewWithConfig(ctx, cfg, opts...)
}

// Create pool with pgx config object
func NewWithConfig(ctx context.Context, config *pgxpool.Config, opts ...PoolOption) (*Pool, error) {
	pool, err := discover(ctx, config)
	if err != nil {
		return nil, fmt.Errorf("pool: discovery error: %w", err)
	}

	context, cf := context.WithCancel(ctx)

	balancer := newBalancer(pool)
	poller := newManager(config, balancer)
	poller.StartPolling(context, pollPeriod)

	connPool := &Pool{ctx: context, cf: cf, masterCfg: config, balancer: balancer, manager: poller}
	for _, opt := range opts {
		opt(connPool)
	}

	return connPool, nil
}

func (p *Pool) Conns() []*pgxpool.Pool {
	return p.balancer.Conns()
}

func (p *Pool) Ping() error {
	//TODO: use standard Ping method when it will be supported
	for _, p := range p.balancer.Conns() {
		if err := pingPool(p, p.Config().ConnConfig.ConnectTimeout); err != nil {
			return err
		}
	}

	return nil
}

func (p *Pool) Query(ctx context.Context, sql string, args ...any) (pgx.Rows, error) {
	conn := p.balancer.NextConnection()
	return conn.Query(ctx, sql, args...)
}

func (p *Pool) QueryRow(ctx context.Context, sql string, args ...any) pgx.Row {
	conn := p.balancer.NextConnection()
	return conn.QueryRow(ctx, sql, args...)
}

func (p *Pool) Exec(ctx context.Context, sql string, args ...any) (pgconn.CommandTag, error) {
	conn := p.balancer.NextConnection()
	return conn.Exec(ctx, sql, args...)
}

func (p *Pool) Close() {
	p.cf()
	for _, pool := range p.balancer.Conns() {
		pool.Close()
	}
}

func (p *Pool) Reset() {
	for _, c := range p.balancer.Conns() {
		c.Reset()
	}
}

func pingPool(pool *pgxpool.Pool, timeout time.Duration) error {
	// Acquire a connection immediately after creating the pool to check
	// if a connection can successfully be established.
	ctx, cf := context.WithTimeout(context.Background(), timeout)
	defer cf()
	if conn, err := pool.Acquire(ctx); err != nil {
		return err
	} else {
		conn.Release()
	}

	return nil
}
