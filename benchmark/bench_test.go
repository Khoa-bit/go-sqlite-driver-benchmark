package benchmark

import (
	"context"
	"fmt"
	"khoa-sqlite-driver-benchmark/modernc"
	"khoa-sqlite-driver-benchmark/tool"
	"khoa-sqlite-driver-benchmark/zombie"
	"math/rand"
	"os"
	"path/filepath"
	"runtime"
	"sync"
	"testing"
	"time"
)

const seedRows = 10000 // number of rows to pre-populate for read benchmarks

// ────────────────────────────  Test helpers  ────────────────────────────

// tempDBPath returns a path in the OS temp directory for a benchmark DB.
func tempDBPath(name string) string {
	return filepath.Join(os.TempDir(), fmt.Sprintf("bench_%s_%d.db", name, time.Now().UnixNano()))
}

// setupDrivers creates two independent Driver instances (one per driver)
// with fresh database files, creates the schema, and seeds seedRows rows.
// Returns both drivers, a cleanup function, and the seeded IDs.
func setupDrivers(tb testing.TB) (zombieDrv Driver, moderncDrv Driver, cleanup func(), ids []int64) {
	tb.Helper()

	ctx := context.Background()

	// --- Zombie ---
	zombiePath := tempDBPath("zombie")
	zombiePool := zombie.SetupMainPool(ctx, zombiePath)
	zombieDrv = NewZombieDriver(zombiePool)

	wc, wdone := zombiePool.TakeWriteConn(ctx)
	err := CreateSchemaZombie(wc.Conn)
	tool.Assert(err == nil, "failed to create zombie schema", "error", err)
	ids, err = SeedDataZombie(wc.Conn, seedRows)
	tool.Assert(err == nil, "failed to seed zombie data", "error", err)
	wdone()

	// --- Modernc ---
	moderncPath := tempDBPath("modernc")
	moderncPool := modernc.SetupModerncPool(ctx, moderncPath)
	moderncDrv = NewModerncDriver(moderncPool)

	err = CreateSchemaModernc(moderncPool.WriteDB)
	tool.Assert(err == nil, "failed to create modernc schema", "error", err)
	_, err = SeedDataModernc(moderncPool.WriteDB, seedRows)
	tool.Assert(err == nil, "failed to seed modernc data", "error", err)

	cleanup = func() {
		zombiePool.CloseAll()
		moderncPool.CloseAll()
		os.Remove(zombiePath)
		os.Remove(zombiePath + "-shm")
		os.Remove(zombiePath + "-wal")
		os.Remove(moderncPath)
		os.Remove(moderncPath + "-shm")
		os.Remove(moderncPath + "-wal")
	}

	return zombieDrv, moderncDrv, cleanup, ids
}

// runBoth runs fn once for each driver as a sub-benchmark.
func runBoth(b *testing.B, zombieDrv, moderncDrv Driver, fn func(*testing.B, Driver)) {
	b.Helper()
	b.Run("zombie", func(b *testing.B) { fn(b, zombieDrv) })
	b.Run("modernc", func(b *testing.B) { fn(b, moderncDrv) })
	b.StopTimer()
}

// ────────────────────────────  Benchmarks  ────────────────────────────

func BenchmarkInsert_Single(b *testing.B) {
	zombieDrv, moderncDrv, cleanup, _ := setupDrivers(b)
	defer cleanup()

	runBoth(b, zombieDrv, moderncDrv, func(b *testing.B, drv Driver) {
		ctx := context.Background()
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			_, err := drv.Insert(ctx,
				fmt.Sprintf("bench_%d", i),
				fmt.Sprintf("bench_%d@example.com", i),
				18+(i%60),
				float64(i)*1.5,
			)
			if err != nil {
				b.Fatal(err)
			}
		}
	})
}

func BenchmarkInsert_Batch(b *testing.B) {
	zombieDrv, moderncDrv, cleanup, _ := setupDrivers(b)
	defer cleanup()

	batchSize := 1000
	now := time.Now().UTC().Format(time.RFC3339)

	runBoth(b, zombieDrv, moderncDrv, func(b *testing.B, drv Driver) {
		ctx := context.Background()
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			b.StopTimer()
			rows := make([]UserRow, batchSize)
			base := i * batchSize
			for j := 0; j < batchSize; j++ {
				rows[j] = UserRow{
					Name:      fmt.Sprintf("batch_%d_%d", base, j),
					Email:     fmt.Sprintf("batch_%d_%d@example.com", base, j),
					Age:       18 + (j % 60),
					Balance:   float64(j) * 1.5,
					CreatedAt: now,
					UpdatedAt: now,
				}
			}
			b.StartTimer()
			if err := drv.BatchInsert(ctx, rows); err != nil {
				b.Fatal(err)
			}
		}
	})
}

func BenchmarkSelect_ByPK(b *testing.B) {
	zombieDrv, moderncDrv, cleanup, ids := setupDrivers(b)
	defer cleanup()

	runBoth(b, zombieDrv, moderncDrv, func(b *testing.B, drv Driver) {
		ctx := context.Background()
		rng := rand.New(rand.NewSource(42))
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			id := ids[rng.Intn(len(ids))]
			_, err := drv.SelectByPK(ctx, id)
			if err != nil {
				b.Fatal(err)
			}
		}
	})
}

func BenchmarkSelect_RangeScan(b *testing.B) {
	zombieDrv, moderncDrv, cleanup, _ := setupDrivers(b)
	defer cleanup()

	runBoth(b, zombieDrv, moderncDrv, func(b *testing.B, drv Driver) {
		ctx := context.Background()
		rng := rand.New(rand.NewSource(42))
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			minAge := rng.Intn(60) + 18
			maxAge := minAge + rng.Intn(20)
			_, err := drv.SelectByAgeRange(ctx, minAge, maxAge)
			if err != nil {
				b.Fatal(err)
			}
		}
	})
}

func BenchmarkUpdate(b *testing.B) {
	zombieDrv, moderncDrv, cleanup, ids := setupDrivers(b)
	defer cleanup()

	runBoth(b, zombieDrv, moderncDrv, func(b *testing.B, drv Driver) {
		ctx := context.Background()
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			id := ids[i%len(ids)]
			newEmail := fmt.Sprintf("updated_%d@example.com", i)
			if err := drv.UpdateEmail(ctx, id, newEmail); err != nil {
				b.Fatal(err)
			}
		}
	})
}

func BenchmarkDelete(b *testing.B) {
	zombieDrv, moderncDrv, cleanup, _ := setupDrivers(b)
	defer cleanup()

	runBoth(b, zombieDrv, moderncDrv, func(b *testing.B, drv Driver) {
		ctx := context.Background()
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			b.StopTimer()
			// Re-insert a row to delete so the table doesn't drain.
			id, err := drv.Insert(ctx,
				fmt.Sprintf("del_%d", i),
				fmt.Sprintf("del_%d@example.com", i),
				30, 0.0,
			)
			if err != nil {
				b.Fatal(err)
			}
			b.StartTimer()

			if err := drv.Delete(ctx, id); err != nil {
				b.Fatal(err)
			}
		}
	})
}

func BenchmarkConcurrentReads(b *testing.B) {
	zombieDrv, moderncDrv, cleanup, ids := setupDrivers(b)
	defer cleanup()

	concurrency := runtime.NumCPU()

	runBoth(b, zombieDrv, moderncDrv, func(b *testing.B, drv Driver) {
		ctx := context.Background()
		rng := rand.New(rand.NewSource(42))
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			// Pick N IDs for this iteration (one per goroutine * 10 ops each).
			workIds := make([]int64, concurrency*10)
			for j := range workIds {
				workIds[j] = ids[rng.Intn(len(ids))]
			}
			if err := drv.ConcurrentReads(ctx, workIds, concurrency); err != nil {
				b.Fatal(err)
			}
		}
	})
}

func BenchmarkWriteThroughput(b *testing.B) {
	zombieDrv, moderncDrv, cleanup, _ := setupDrivers(b)
	defer cleanup()

	concurrency := runtime.NumCPU()

	runBoth(b, zombieDrv, moderncDrv, func(b *testing.B, drv Driver) {
		ctx := context.Background()
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			var wg syncErrGroup
			for g := 0; g < concurrency; g++ {
				g := g
				wg.Go(func() error {
					_, err := drv.Insert(ctx,
						fmt.Sprintf("write_%d_%d", i, g),
						fmt.Sprintf("write_%d_%d@example.com", i, g),
						25+g, float64(g),
					)
					return err
				})
			}
			if err := wg.Wait(); err != nil {
				b.Fatal(err)
			}
		}
	})
}

func BenchmarkContention_WriteRead(b *testing.B) {
	zombieDrv, moderncDrv, cleanup, ids := setupDrivers(b)
	defer cleanup()

	readerCount := runtime.NumCPU()
	opsPerReader := 50       // reader-heavy: each reader does 50 reads
	writerOps := readerCount // writer does 1 insert per reader

	runBoth(b, zombieDrv, moderncDrv, func(b *testing.B, drv Driver) {
		ctx := context.Background()
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			if err := drv.ContentionWriteRead(ctx, ids, readerCount, opsPerReader, writerOps); err != nil {
				b.Fatal(err)
			}
		}
	})
}

func BenchmarkTx_WriteOnly(b *testing.B) {
	zombieDrv, moderncDrv, cleanup, _ := setupDrivers(b)
	defer cleanup()

	runBoth(b, zombieDrv, moderncDrv, func(b *testing.B, drv Driver) {
		ctx := context.Background()
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			if err := drv.TxWriteOnly(ctx); err != nil {
				b.Fatal(err)
			}
		}
	})
}

func BenchmarkTx_ReadWrite(b *testing.B) {
	zombieDrv, moderncDrv, cleanup, ids := setupDrivers(b)
	defer cleanup()

	runBoth(b, zombieDrv, moderncDrv, func(b *testing.B, drv Driver) {
		ctx := context.Background()
		rng := rand.New(rand.NewSource(42))
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			id := ids[rng.Intn(len(ids))]
			newEmail := fmt.Sprintf("tx_rw_%d@example.com", i)
			if err := drv.TxReadWrite(ctx, id, newEmail); err != nil {
				b.Fatal(err)
			}
		}
	})
}

// ────────────────────────────  syncErrGroup (tiny helper)  ────────────────────────────

type syncErrGroup struct {
	wg   sync.WaitGroup
	mu   sync.Mutex
	errs []error
}

func (g *syncErrGroup) Go(fn func() error) {
	g.wg.Add(1)
	go func() {
		defer g.wg.Done()
		if err := fn(); err != nil {
			g.mu.Lock()
			g.errs = append(g.errs, err)
			g.mu.Unlock()
		}
	}()
}

func (g *syncErrGroup) Wait() error {
	g.wg.Wait()
	if len(g.errs) > 0 {
		return g.errs[0]
	}
	return nil
}
