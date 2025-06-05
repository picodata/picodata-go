package picodata

import (
	"context"
	"fmt"
	"sync"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"
)

// Pool allows for connection reuse.
// It operates with slice of *pgxpool.Pool, each Picodata instance have its own pool object.
type Pool struct {
	provider *connectionProvider
	manager  *topologyManager
	producer *stateProducer

	stopOnce sync.Once
	stopChan chan struct{}
}

// ParseConfig builds a [*pgxpool.Config] from connString.
//
//	Example URL
//	postgres://admin:T0psecret@localhost:5432/sslmode=disable&pool_max_conns=10&pool_max_conn_lifetime=1h30m
func ParseConfig(connString string) (*pgxpool.Config, error) {
	return pgxpool.ParseConfig(connString)
}

// New creates a new [Pool]. See [ParseConfig] for information on connString format.
func New(ctx context.Context, connString string, opts ...PoolOption) (*Pool, error) {
	cfg, err := pgxpool.ParseConfig(connString)
	if err != nil {
		return nil, err
	}

	return NewWithConfig(ctx, cfg, opts...)
}

// NewWithConfig creates a new [Pool]. config must have been created by [ParseConfig].
func NewWithConfig(ctx context.Context, config *pgxpool.Config, opts ...PoolOption) (*Pool, error) {
	initConn, err := pgxpool.NewWithConfig(ctx, config)
	if err != nil {
		return nil, fmt.Errorf("pool: NewWithConfig: %v", err)
	}

	// TODO: research the most suitable capacity
	eventChan := make(chan event, 10)
	stopChan := make(chan struct{})

	provider := newConnectionProvider(initConn)

	manager := newTopologyManager(provider)
	go manager.runProcessing(eventChan)

	producer := newStateProducer(provider)
	go producer.runProducing(eventChan, stopChan)

	connPool := &Pool{provider: provider, manager: manager, producer: producer, stopChan: stopChan}

	for _, opt := range opts {
		if err := opt(connPool); err != nil {
			return nil, fmt.Errorf("pool: NewWithConfig: error applying option: %v", err)
		}
	}

	return connPool, nil
}

// Config returns a copy of config that was used to initialize this pool.
func (p *Pool) Config() *pgxpool.Config {
	return p.provider.config()
}

// Ping acquires a connection from the Pool and executes a simple SQL statement against it.
// If the sql returns without error, the database Ping is considered successful, otherwise, the error is returned.
func (p *Pool) Ping(ctx context.Context) error {
	// TODO: use standard Ping method when it will be supported
	for _, pool := range p.provider.conns() {
		if err := pingPool(ctx, pool); err != nil {
			return err
		}
	}

	return nil
}

// Query acquires a connection and executes a query that returns pgx.Rows.
// Arguments should be referenced positionally from the SQL string as $1, $2, etc.
// See pgx.Rows documentation to close the returned Rows and return the acquired connection to the Pool.
//
// If there is an error, the returned pgx.Rows will be returned in an error state.
// If preferred, ignore the error returned from Query and handle errors using the returned pgx.Rows.
//
// For extra control over how the query is executed, the types QuerySimpleProtocol, QueryResultFormats, and
// QueryResultFormatsByOID may be used as the first args to control exactly how the query is executed. This is rarely
// needed. See the documentation for those types for details.
func (p *Pool) Query(ctx context.Context, sql string, args ...any) (pgx.Rows, error) {
	conn := p.provider.nextConnection()
	return conn.Query(ctx, sql, args...)
}

// QueryRow acquires a connection and executes a query that is expected
// to return at most one row (pgx.Row). Errors are deferred until pgx.Row's
// Scan method is called. If the query selects no rows, pgx.Row's Scan will
// return ErrNoRows. Otherwise, pgx.Row's Scan scans the first selected row
// and discards the rest. The acquired connection is returned to the Pool when
// pgx.Row's Scan method is called.
//
// Arguments should be referenced positionally from the SQL string as $1, $2, etc.
//
// For extra control over how the query is executed, the types QuerySimpleProtocol, QueryResultFormats, and
// QueryResultFormatsByOID may be used as the first args to control exactly how the query is executed. This is rarely
// needed. See the documentation for those types for details.
func (p *Pool) QueryRow(ctx context.Context, sql string, args ...any) pgx.Row {
	conn := p.provider.nextConnection()
	return conn.QueryRow(ctx, sql, args...)
}

// Exec acquires a connection from the Pool and executes the given SQL.
// SQL can be either a prepared statement name or an SQL string.
// Arguments should be referenced positionally from the SQL string as $1, $2, etc.
// The acquired connection is returned to the pool when the Exec function returns.
func (p *Pool) Exec(ctx context.Context, sql string, args ...any) (pgconn.CommandTag, error) {
	conn := p.provider.nextConnection()
	return conn.Exec(ctx, sql, args...)
}

// Close closes all connections in the pool and rejects future Acquire calls. Blocks until all connections are returned
// to pool and closed.
//
// It is safe to close a pool multiple times.
func (p *Pool) Close() {
	p.stopOnce.Do(func() {
		close(p.stopChan)
		for _, pool := range p.provider.conns() {
			pool.Close()
		}
	})
}

// Reset closes all connections, but leaves the pool open. It is intended for use when an error is detected that would
// disrupt all connections (such as a network interruption or a server state change).
//
// It is safe to reset a pool while connections are checked out. Those connections will be closed when they are returned
// to the pool.
func (p *Pool) Reset() {
	for _, c := range p.provider.conns() {
		c.Reset()
	}
}

func pingPool(ctx context.Context, p *pgxpool.Pool) error {
	c, err := p.Acquire(ctx)
	if err != nil {
		return err
	}
	defer c.Release()

	if _, err = c.Exec(ctx, "SELECT 1"); err != nil {
		return fmt.Errorf("pool: pingPool: %w", err)
	}

	return nil
}
