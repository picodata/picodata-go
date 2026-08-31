// Package testenv holds the helpers shared by the tests that need a running
// picodata cluster.
package testenv

import (
	"context"
	"fmt"
	"net"
	"os"
	"strings"
	"testing"
	"time"

	"git.picodata.io/core/drivers/picodata-go/topology"
	"git.picodata.io/core/drivers/picodata-go/topology/source/dynamic"

	"github.com/jackc/pgx/v5"
)

const (
	// defaultURI points at the compose cluster from build/docker-compose.yml.
	defaultURI = "postgresql://admin:T0psecret@picodata-1-1:5432?sslmode=disable"

	readyTimeout  = 60 * time.Second
	readyInterval = 500 * time.Millisecond
)

// URI returns the connection string of the test cluster.
func URI() string {
	if uri, ok := os.LookupEnv("PICODATA_TEST_CONN"); ok {
		return uri
	}

	return defaultURI
}

// RequireCluster skips tests that need a running picodata cluster. Those only
// work inside the cluster network, so plain `go test` on the host runs them
// with -short (see make test-unit).
func RequireCluster(t *testing.T) {
	t.Helper()

	if testing.Short() {
		t.Skip("needs a running picodata cluster: run make test")
	}
}

// WaitReady blocks until the cluster reports at least minOnline online
// instances spread over at least minTiers distinct tiers, and returns that
// topology.
//
// Accepting pgproto connections is not enough to start asserting on the
// cluster: an instance that has joined but not yet caught up is reported
// Offline, and its tier is then missing from the topology. Offline instances
// are otherwise ignored - an instance that died for good stays in the cluster
// forever, and waiting for it would never end.
func WaitReady(t *testing.T, ctx context.Context, minOnline, minTiers int) *topology.ClusterTopology {
	t.Helper()

	config, err := pgx.ParseConfig(URI())
	if err != nil {
		t.Fatalf("parse %s: %v", URI(), err)
	}

	source := dynamic.New(config, []string{net.JoinHostPort(config.Host, fmt.Sprintf("%d", config.Port))})

	deadline := time.Now().Add(readyTimeout)

	var cluster *topology.ClusterTopology
	for {
		cluster, err = source.Fetch(ctx)
		if err == nil && ready(cluster, minOnline, minTiers) {
			return cluster
		}

		if time.Now().After(deadline) {
			t.Fatalf("cluster is not ready after %s: %s", readyTimeout, describe(cluster, err))
		}

		select {
		case <-ctx.Done():
			t.Fatalf("cluster is not ready: %v: %s", ctx.Err(), describe(cluster, err))
		case <-time.After(readyInterval):
		}
	}
}

func ready(cluster *topology.ClusterTopology, minOnline, minTiers int) bool {
	if cluster == nil {
		return false
	}

	tiers := make(map[string]struct{}, minTiers)

	var online int
	for _, node := range cluster.Nodes {
		if node.State != topology.StateOnline {
			continue
		}

		online++
		tiers[node.Tier] = struct{}{}
	}

	return online >= minOnline && len(tiers) >= minTiers
}

// describe renders what the last fetch saw, so a timeout says which instance
// held the cluster back.
func describe(cluster *topology.ClusterTopology, err error) string {
	if err != nil {
		return fmt.Sprintf("last fetch failed: %v", err)
	}

	if cluster == nil || len(cluster.Nodes) == 0 {
		return "no nodes discovered"
	}

	nodes := make([]string, 0, len(cluster.Nodes))
	for _, node := range cluster.Nodes {
		nodes = append(nodes, fmt.Sprintf("%s tier=%q state=%q", node.Address, node.Tier, node.State))
	}

	return strings.Join(nodes, "; ")
}
