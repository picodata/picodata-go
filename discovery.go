package picodata

import (
	"context"
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
)

func discover(ctx context.Context, cfg *pgxpool.Config) ([]*pgxpool.Pool, error) {
	if cfg.ConnConfig.ConnectTimeout == 0 {
		cfg.ConnConfig.ConnectTimeout = time.Duration(1 * time.Second)
	}

	tempPool, err := pgxpool.NewWithConfig(ctx, cfg)
	if err != nil {
		return nil, err
	}
	if err := pingPool(tempPool, cfg.ConnConfig.ConnectTimeout); err != nil {
		return nil, err
	}

	rows, err := tempPool.Query(ctx,
		fmt.Sprintf("SELECT %s FROM %s WHERE %s='pgproto';", addressColumn, peerAddressTable, connTypeColumn),
	)
	if err != nil {
		return nil, err
	}

	var fetchedAddrs []string
	for rows.Next() {
		var addr string

		err := rows.Scan(&addr)
		if err != nil {
			return nil, err
		}
		fetchedAddrs = append(fetchedAddrs, addr)
	}
	rows.Close()
	tempPool.Close()

	connPool := make([]*pgxpool.Pool, 0, len(fetchedAddrs))
	for _, addr := range fetchedAddrs {
		poolCfg := cfg.Copy()
		// Insert instance's host:port into the clonned config object
		host := strings.Split(addr, ":")
		poolCfg.ConnConfig.Host = host[0]
		port, _ := strconv.Atoi(host[1])
		poolCfg.ConnConfig.Port = uint16(port)

		pool, err := pgxpool.NewWithConfig(ctx, poolCfg)
		if err != nil {
			return nil, err
		}
		if err := pingPool(pool, poolCfg.ConnConfig.ConnectTimeout); err != nil {
			return nil, err
		}
		connPool = append(connPool, pool)
	}

	return connPool, nil

}
