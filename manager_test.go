package picodata

import (
	"context"
	"os"
	"sync"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/network"
	"github.com/testcontainers/testcontainers-go/wait"
)

func TestManager(t *testing.T) {
	// Run locally
	if !*ciFlag {
		runManagerTestContainers(t)
		return
	}

	// Run in CI
	runManagerTestCI(t)
}

func runManagerTestContainers(t *testing.T) {
	newNetwork, err := network.New(context.Background())
	require.NoError(t, err)
	testcontainers.CleanupNetwork(t, newNetwork)

	networkName := newNetwork.Name

	req := testcontainers.ContainerRequest{
		Image:    "docker-public.binary.picodata.io/picodata:master",
		Name:     "picodata-1-1",
		Hostname: "picodata-1-1",
		Env: map[string]string{
			"PICODATA_PEER":           "picodata-1-1:3301",
			"PICODATA_LISTEN":         "picodata-1-1:3301",
			"PICODATA_ADVERTISE":      "picodata-1-1:3301",
			"PICODATA_PG_LISTEN":      "0.0.0.0:55432",
			"PICODATA_ADMIN_PASSWORD": adminPassword,
			"PICODATA_LOG_LEVEL":      "info",
		},
		ExposedPorts: []string{"55432:55432"},
		WaitingFor:   wait.ForLog("Discovery enters idle mode, all buckets are known. Discovery works with 10 seconds interval now"),
		Networks:     []string{networkName},
	}
	c1, err := testcontainers.GenericContainer(context.Background(), testcontainers.GenericContainerRequest{
		ContainerRequest: req,
		Started:          true,
	})
	assert.NoError(t, err)

	req2 := testcontainers.ContainerRequest{
		Image:    "docker-public.binary.picodata.io/picodata:master",
		Name:     "picodata-2-1",
		Hostname: "picodata-2-1",
		Env: map[string]string{
			"PICODATA_PEER":           "picodata-1-1:3301",
			"PICODATA_LISTEN":         "picodata-2-1:3301",
			"PICODATA_ADVERTISE":      "picodata-2-1:3301",
			"PICODATA_PG_LISTEN":      "0.0.0.0:55433",
			"PICODATA_ADMIN_PASSWORD": adminPassword,
			"PICODATA_LOG_LEVEL":      "info",
		},
		ExposedPorts: []string{"55433:55433"},
		WaitingFor:   wait.ForLog("Discovery enters idle mode, all buckets are known. Discovery works with 10 seconds interval now"),
		Networks:     []string{networkName},
	}

	c2, err := testcontainers.GenericContainer(context.Background(), testcontainers.GenericContainerRequest{
		ContainerRequest: req2,
		Started:          true,
	})
	assert.NoError(t, err)

	cfg, err := ParseConfig(createPsql(adminPassword, "0.0.0.0:55432"))
	assert.NoError(t, err)

	pool, err := pgxpool.NewWithConfig(context.Background(), cfg)
	assert.NoError(t, err)

	eventChan := make(chan event, 10)

	prov := newConnectionProvider(pool)
	manager := newTopologyManager(prov)
	go manager.runProcessing(eventChan)

	// Send events to manager
	addrs := []string{"0.0.0.0:55432", "0.0.0.0:55433"}
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
		return
	}()
	wg.Wait()

	assert.Len(t, prov.conns(), 2)

	err = c1.Terminate(context.Background())
	assert.NoError(t, err)
	err = c2.Terminate(context.Background())
	assert.NoError(t, err)
}

func runManagerTestCI(t *testing.T) {
	cfg, err := pgxpool.ParseConfig(createPsql(os.Getenv("PICODATA_ADMIN_PASSWORD"), "picodata-1:5432"))
	assert.NoError(t, err)

	pool, err := pgxpool.NewWithConfig(context.Background(), cfg)
	assert.NoError(t, err)

	eventChan := make(chan event, 10)

	prov := newConnectionProvider(pool)
	manager := newTopologyManager(prov)
	go manager.runProcessing(eventChan)

	// Send events to manager
	addrs := []string{"picodata-1:5432", "picodata-2:5432"}
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
		return
	}()
	wg.Wait()

	assert.Len(t, prov.conns(), 2)
}
