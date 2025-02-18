package picodata

import (
	"context"
	"flag"
	"fmt"
	"os"
	"testing"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/network"
	"github.com/testcontainers/testcontainers-go/wait"
)

const (
	adminUsername = "admin"
	adminPassword = "T0psecret"
)

var (
	ciFlag = flag.Bool("ci", false, "define is test has been running in CI")
)

func TestDiscovery(t *testing.T) {
	// Run locally
	if !*ciFlag {
		runTestContainers(t)
		return
	}

	// Run in CI
	runTestDiscovery(t)
}

func createPsql(pass, host string) string {
	return fmt.Sprintf("postgres://%s:%s@%s?sslmode=disable", adminUsername, pass, host)
}

func runTestContainers(t *testing.T) {
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

	cfg, err := pgxpool.ParseConfig(createPsql(adminPassword, "0.0.0.0:55432"))
	assert.NoError(t, err)

	ctx := context.Background()

	pool, err := discover(ctx, cfg)
	assert.NoError(t, err)
	assert.Equal(t, len(pool), 2)
	addrs := []string{}
	for _, conn := range pool {
		addrs = append(addrs, fmt.Sprintf("%s:%d", conn.Config().ConnConfig.Host, conn.Config().ConnConfig.Port))
	}
	assert.Contains(t, addrs, "0.0.0.0:55432")
	assert.Contains(t, addrs, "0.0.0.0:55433")

	err = c1.Terminate(context.Background())
	assert.NoError(t, err)
	err = c2.Terminate(context.Background())
	assert.NoError(t, err)
}

func runTestDiscovery(t *testing.T) {
	ctx := context.Background()

	cfg, err := pgxpool.ParseConfig(createPsql(os.Getenv("PICODATA_ADMIN_PASSWORD"), "picodata-1:5432"))
	assert.NoError(t, err)
	pool, err := discover(ctx, cfg)
	assert.NoError(t, err)

	assert.Equal(t, len(pool), 2)
	addrs := []string{}
	for _, conn := range pool {
		addrs = append(addrs, fmt.Sprintf("%s:%d", conn.Config().ConnConfig.Host, conn.Config().ConnConfig.Port))
	}
	assert.Contains(t, addrs, "picodata-1:5432")
	assert.Contains(t, addrs, "picodata-2:5432")
}
