package polling

import (
	"context"
	"time"

	"git.picodata.io/core/drivers/picodata-go/logger"
	"git.picodata.io/core/drivers/picodata-go/topology"
	"git.picodata.io/core/drivers/picodata-go/topology/source/dynamic"
	"github.com/jackc/pgx/v5"
)

// Source wraps DynamicSource to provide periodic topology delta updates.
type Source struct {
	dynamic  *dynamic.Source
	interval time.Duration
}

// New creates a new PollingSource initializing the underlying dynamic explorer.
func New(baseConfig *pgx.ConnConfig, bootstrapAddrs []string, interval time.Duration) *Source {
	return &Source{
		dynamic:  dynamic.New(baseConfig, bootstrapAddrs),
		interval: interval,
	}
}

// Fetch pulls the full current topology from the cluster.
func (p *Source) Fetch(ctx context.Context) (*topology.ClusterTopology, error) {
	return p.dynamic.Fetch(ctx)
}

// Subscribe starts an internal polling loop and returns a channel streaming raw cluster topologies.
func (p *Source) Subscribe(ctx context.Context) (<-chan *topology.ClusterTopology, error) {
	// 10 elements buffer to prevent stalling the internal ticker
	ch := make(chan *topology.ClusterTopology, 10)

	go func() {
		defer close(ch)

		ticker := time.NewTicker(p.interval)
		defer ticker.Stop()

		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				topology, err := p.dynamic.Fetch(ctx)
				if err != nil {
					logger.Log(logger.LevelWarn, "[topology] polling source: background fetch error: %v", err)
					continue
				}

				select {
				case ch <- topology:
				default:
					logger.Log(logger.LevelWarn, "[topology] polling source: buffer full")
				}
			}
		}
	}()

	return ch, nil
}
