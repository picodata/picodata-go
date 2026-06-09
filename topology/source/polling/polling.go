package polling

import (
	"context"
	"log/slog"
	"time"

	"git.picodata.io/core/drivers/picodata-go/topology"
	"git.picodata.io/core/drivers/picodata-go/topology/source/dynamic"
	"github.com/jackc/pgx/v5"
)

// Source wraps DynamicSource to provide periodic topology delta updates.
type Source struct {
	logger   *slog.Logger
	dynamic  *dynamic.Source
	interval time.Duration
}

// Option applies a configuration option to the Polling Source.
type Option func(*Source)

// WithLogger takes the root logger and wraps it with the polling package tag.
func WithLogger(l *slog.Logger) Option {
	return func(s *Source) {
		s.logger = l.With(slog.String("pkg", "topology.polling"))
	}
}

// New creates a new PollingSource initializing the underlying dynamic explorer.
func New(baseConfig *pgx.ConnConfig, bootstrapAddrs []string, interval time.Duration, opts ...Option) *Source {
	s := &Source{
		logger:   slog.New(slog.DiscardHandler),
		interval: interval,
	}

	for _, opt := range opts {
		opt(s)
	}

	s.dynamic = dynamic.New(baseConfig, bootstrapAddrs, dynamic.WithLogger(s.logger))
	return s
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
					p.logger.Warn("background fetch error", slog.Any("error", err))
					continue
				}

				select {
				case ch <- topology:
				default:
					p.logger.Warn("buffer full")
				}
			}
		}
	}()

	return ch, nil
}
