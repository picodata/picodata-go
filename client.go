package picodata

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net"
	"sync"
	"time"

	"git.picodata.io/core/drivers/picodata-go/topology"
	"git.picodata.io/core/drivers/picodata-go/topology/source/dynamic"
	"git.picodata.io/core/drivers/picodata-go/topology/source/polling"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"
)

// TopologyPollTimeoute setup topology update time
const TopologyPollTimeoute = 5000 * time.Millisecond

// Client allows for connection reuse.
// It operates with slice of *pgxpool.Client, each Picodata instance have its own pool object.
type Client struct {
	pools   *poolManager
	router  *router
	manager *topology.Manager

	stopOnce sync.Once
}

// ErrNoConnections is returned when no connections are available in the pool.
var ErrNoConnections = errors.New("no available connections")

// errorRow implements pgx.Row and returns a predefined error on Scan.
// Used to avoid returning nil from QueryRow when no connections are available.
type errorRow struct {
	err error
}

func (r errorRow) Scan(_ ...any) error {
	return r.err
}

// errorBatchResults implements pgx.BatchResults and returns a predefined error
// on any method call. Used to avoid returning nil from SendBatch.
type errorBatchResults struct {
	err error
}

func (b errorBatchResults) Close() error {
	return b.err
}

func (b errorBatchResults) Exec() (pgconn.CommandTag, error) {
	return pgconn.CommandTag{}, b.err
}

func (b errorBatchResults) Query() (pgx.Rows, error) {
	return nil, b.err
}

func (b errorBatchResults) QueryRow() pgx.Row {
	return errorRow(b)
}

// ParseConfig builds a [*pgxpool.Config] from connString.
//
//	Example URL
//	postgres://admin:T0psecret@localhost:5432?sslmode=disable&pool_max_conns=10
func ParseConfig(connString string) (*pgxpool.Config, error) {
	return pgxpool.ParseConfig(connString)
}

// New creates a new [Client]. See [ParseConfig] for information on connString format.
func New(ctx context.Context, connString string, opts ...PoolOption) (*Client, error) {
	cfg, err := pgxpool.ParseConfig(connString)
	if err != nil {
		return nil, err
	}

	return NewWithConfig(ctx, cfg, opts...)
}

// NewWithConfig creates a new [Client]. config must have been created by [ParseConfig].
func NewWithConfig(ctx context.Context, config *pgxpool.Config, opts ...PoolOption) (*Client, error) {
	poolOpts := &clientOpts{}
	for i, opt := range opts {
		if err := opt(poolOpts); err != nil {
			return nil, fmt.Errorf("apply config error [param=%d]: %w", i+1, err)
		}
	}

	logger := poolOpts.logger
	if logger == nil {
		logger = slog.New(slog.DiscardHandler)
	}

	// Extract the primary host from DSN if it exists
	if config.ConnConfig.Host != "" {
		primaryAddr := net.JoinHostPort(config.ConnConfig.Host, fmt.Sprintf("%d", config.ConnConfig.Port))
		poolOpts.bootstrapHosts = append(poolOpts.bootstrapHosts, primaryAddr)
	}

	initConn, err := pgxpool.NewWithConfig(ctx, config)
	if err != nil {
		return nil, fmt.Errorf("pgxpoll init error: %w", err)
	}

	var connPool *Client
	pools := newPoolManager(initConn, poolOpts.maxConnsPerInstance, withPoolManagerLogger(logger))

	routerOpts := []routerOption{withRouterLogger(logger)}
	if poolOpts.balanceStrategy != nil {
		routerOpts = append(routerOpts, withBalanceStrategy(poolOpts.balanceStrategy))
	}
	router := newRouter(pools, routerOpts...)

	var topologySource topology.Source
	switch {
	case poolOpts.topologySource != nil:
		topologySource = poolOpts.topologySource
	case !poolOpts.disableTopologyUpdate:
		topologySource = polling.New(config.ConnConfig, poolOpts.bootstrapHosts, TopologyPollTimeoute, polling.WithLogger(logger))
	default:
		topologySource = dynamic.New(config.ConnConfig, poolOpts.bootstrapHosts, dynamic.WithLogger(logger))
	}

	topologyManager, err := topology.NewTopologyManager(ctx, topologySource, topology.WithLogger(logger))
	if err != nil {
		return nil, fmt.Errorf("topology manager init error: %w", err)
	}

	topologyManager.AddListener(pools)

	connPool = &Client{pools: pools, router: router, manager: topologyManager}

	return connPool, nil
}

// Config returns a copy of config that was used to initialize this pool.
func (p *Client) Config() *pgxpool.Config {
	return p.pools.getConfig()
}

// Ping acquires a connection from the Pool and executes a simple SQL statement against it.
// If the sql returns without error, the database Ping is considered successful, otherwise, the error is returned.
//
// Deprecated: usefulness of a cluster-wide ping is unclear; may be removed or reworked.
func (p *Client) Ping(ctx context.Context) error {
	// TODO: https://git.picodata.io/core/picodata/-/issues/1324
	// The Picodata SQL layer (sbroad) doesn't support comments parsing.
	// The original *pgxpool.Ping() method sends an empty query, **--ping**, which is a comment.
	// We need to use a custom function to send a simple **SELECT 1** query instead.
	// Replace to original Ping method when comment support is implemented.
	return p.pools.ping(ctx)
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
func (p *Client) Query(ctx context.Context, sql string, args ...any) (pgx.Rows, error) {
	conn := p.router.nextConnection()
	if conn == nil {
		return nil, ErrNoConnections
	}
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
func (p *Client) QueryRow(ctx context.Context, sql string, args ...any) pgx.Row {
	conn := p.router.nextConnection()
	if conn == nil {
		return errorRow{err: ErrNoConnections}
	}
	return conn.QueryRow(ctx, sql, args...)
}

// SendBatch acquires a connection from the pool and sends a batch of SQL
// commands for execution using that connection.
//
// It returns a pgx.BatchResults interface, which provides access to the
// results of each command in the batch.
//
// If acquiring a connection from the pool fails, SendBatch returns a
// pgx.BatchResults that will return the acquisition error on any result method.
//
// The caller must ensure that the returned BatchResults is closed via
// Close() to release the connection back to the pool.
//
// Example usage
//
//	batch := &pgx.Batch{}
//	batch.Queue("INSERT INTO users (name) VALUES($1)", "alice")
//	batch.Queue("SELECT COUNT(*) FROM users")
//	results := pool.SendBatch(ctx, batch)
//	defer results.Close()
//	tag, err := results.Exec()
//	if err != nil{...}
//	err := results.QueryRow().Scan(&count)
//	if err != nil{...}
func (p *Client) SendBatch(ctx context.Context, b *pgx.Batch) pgx.BatchResults {
	conn := p.router.nextConnection()
	if conn == nil {
		return errorBatchResults{err: ErrNoConnections}
	}
	return conn.SendBatch(ctx, b)
}

// Exec acquires a connection from the Pool and executes the given SQL.
// SQL can be either a prepared statement name or an SQL string.
// Arguments should be referenced positionally from the SQL string as $1, $2, etc.
// The acquired connection is returned to the pool when the Exec function returns.
func (p *Client) Exec(ctx context.Context, sql string, args ...any) (pgconn.CommandTag, error) {
	conn := p.router.nextConnection()
	if conn == nil {
		return pgconn.CommandTag{}, ErrNoConnections
	}
	return conn.Exec(ctx, sql, args...)
}

// Close closes all connections in the pool and rejects future Acquire calls. Blocks until all connections are returned
// to pool and closed.
//
// It is safe to close a pool multiple times.
func (p *Client) Close() {
	p.stopOnce.Do(func() {
		p.manager.Close()
		p.pools.close()
	})
}

// Reset closes all connections, but leaves the pool open. It is intended for use when an error is detected that would
// disrupt all connections (such as a network interruption or a server state change).
//
// It is safe to reset a pool while connections are checked out. Those connections will be closed when they are returned
// to the pool.
//
// Deprecated: usefulness of a cluster-wide reset is unclear; may be removed or reworked.
func (p *Client) Reset() {
	p.pools.reset()
}
