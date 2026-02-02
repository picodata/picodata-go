package picodata

import (
	"context"
	"fmt"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/picodata/picodata-go/logger"
)

// initialDiscovery performs a one-time topology discovery and populates
// the connection provider with all online instances.
func initialDiscovery(ctx context.Context, provider *connectionProvider) error {
	const op = "discovery: initialDiscovery"

	// Query topology
	instances, err := getTopology(ctx, provider.nextConnection())
	if err != nil {
		return fmt.Errorf("%s: %w", op, err)
	}

	// Get initial connection address to skip it
	initConfig := provider.config()
	initAddr := fmt.Sprintf("%s:%d", initConfig.ConnConfig.Host, initConfig.ConnConfig.Port)

	// Add connections for all online instances (except the initial one)
	added := 0
	for _, inst := range instances {
		// Skip non-online instances
		if inst.currentState != stateOnline {
			continue
		}
		// Skip the initial connection (already added)
		if inst.address == initAddr {
			continue
		}

		if err := provider.addConn(inst.address); err != nil {
			logger.Log(logger.LevelError, "%s: failed to add connection for %s: %v", op, inst.address, err)
			continue
		}
		added++
	}

	logger.Log(logger.LevelDebug, "%s: discovered %d instances, added %d connections", op, len(instances), added)

	return nil
}

// getTopology queries the cluster topology and returns all instances
func getTopology(ctx context.Context, conn *pgxpool.Pool) ([]connState, error) {
	const op = "discovery: getTopology"

	instances := make([]connState, 0)

	rows, err := conn.Query(ctx, connsStateQuery)
	if err != nil {
		return nil, fmt.Errorf("%s: %w", op, err)
	}
	defer rows.Close()

	for rows.Next() {
		var connAddr string
		var connFetchedState []any // contains [string, int]

		if err := rows.Scan(&connAddr, &connFetchedState); err != nil {
			return nil, fmt.Errorf("%s: %w", op, err)
		}

		connStateStr, ok := connFetchedState[0].(string)
		if !ok {
			return nil, fmt.Errorf("%s: %s state must be a string, but has type %T", op, connAddr, connFetchedState[0])
		}

		instances = append(instances, connState{address: connAddr, currentState: connStateStr})
	}

	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("%s: %w", op, err)
	}

	return instances, nil
}
