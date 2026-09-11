package picodata

import (
	"runtime"
	"testing"

	"github.com/jackc/pgx/v5/pgxpool"
)

// benchSink keeps the compiler from discarding the calls under measurement.
var benchSink *pgxpool.Pool

// benchPoolManager builds a manager serving three instances, the shape the hot
// path is measured against.
func benchPoolManager(b *testing.B) *poolManager {
	b.Helper()

	pm := newPoolManager(newMockConfig("127.0.0.1", 5432), 1)
	b.Cleanup(pm.close)

	pm.reconcile(snapshot(1,
		node("uuid-0", "127.0.0.1:5433"),
		node("uuid-1", "127.0.0.1:5434"),
		node("uuid-2", "127.0.0.1:5435"),
	))

	return pm
}

// BenchmarkNextConnection measures the whole per-query routing path: the
// snapshot load, the balance strategy, and the slice index.
func BenchmarkNextConnection(b *testing.B) {
	r := newRouter(benchPoolManager(b))

	b.ReportAllocs()

	for b.Loop() {
		benchSink = r.nextConnection()
	}
}

// BenchmarkNextConnectionParallel is where the shared-lock cost used to show:
// readers serialized on the RWMutex reader counter, an atomic load does not.
func BenchmarkNextConnectionParallel(b *testing.B) {
	r := newRouter(benchPoolManager(b))

	b.ReportAllocs()
	b.ResetTimer()

	b.RunParallel(func(pb *testing.PB) {
		var local *pgxpool.Pool

		for pb.Next() {
			local = r.nextConnection()
		}

		// Kept per goroutine: publishing it to the shared sink would be a data
		// race the moment the benchmark is run under -race.
		runtime.KeepAlive(local)
	})
}
