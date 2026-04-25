package picodata

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/stretchr/testify/assert"
)

func TestManager(t *testing.T) {
	cfg, err := pgxpool.ParseConfig(picodataURI())
	assert.NoError(t, err)

	pool, err := pgxpool.NewWithConfig(context.Background(), cfg)
	assert.NoError(t, err)

	eventChan := make(chan event, 10)

	prov := newConnectionProvider(pool, 1)
	manager := newTopologyManager(prov)
	go manager.runProcessing(eventChan)

	// Send events to manager
	addrs := []string{"picodata-2:5432"}
	for _, addr := range addrs {
		eventChan <- event{address: addr, state: stateOnline}
	}
	close(eventChan)

	// Need to wait until manager process all events
	wg := sync.WaitGroup{}
	wg.Add(1)
	go func() {
		time.Sleep(2 * time.Second)
		wg.Done()
	}()
	wg.Wait()

	assert.Len(t, prov.conns(), 2)
}
