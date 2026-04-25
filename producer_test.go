package picodata

import (
	"context"
	"os"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/stretchr/testify/assert"
)

const (
	defaultURI = "postgresql://admin:T0psecret@localhost:55432?sslmode=disable"
)

func picodataURI() string {
	val, ok := os.LookupEnv("PICODATA_TEST_CONN")

	if !ok {
		return defaultURI
	}

	return val
}

func TestProducer(t *testing.T) {
	dbURI := picodataURI()

	cfg, err := pgxpool.ParseConfig(dbURI)
	assert.NoError(t, err)

	pool, err := pgxpool.NewWithConfig(context.Background(), cfg)
	assert.NoError(t, err)

	eventChan := make(chan event, 10)
	eventSlice := make([]event, 0, 10)
	stopChan := make(chan struct{})

	prov := newConnectionProvider(pool, 1)
	producer, err := newStateProducer(prov, dbURI)
	assert.NoError(t, err)
	go producer.runProducing(eventChan, stopChan)

	go func() {
		stop := time.After(2 * time.Second)
		<-stop
		close(stopChan)
	}()

	for event := range eventChan {
		eventSlice = append(eventSlice, event)
	}

	assert.NotEmpty(t, eventSlice)

	statusMap := map[string]int{}

	for _, event := range eventSlice {
		statusMap[event.address]++
	}

	for _, occurence := range statusMap {
		assert.NotZero(t, occurence)
	}
}
