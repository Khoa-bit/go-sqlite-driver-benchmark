package main

import (
	"context"
	"fmt"
	"khoa-sqlite-driver-benchmark/modernc"
	"khoa-sqlite-driver-benchmark/zombie"
)

func main() {
	ctx := context.Background()

	// === Zombie (zombiezen.com/go/sqlite) ===
	zombieMainPool := zombie.SetupMainPool(ctx, "C:/Users/KhoaNguyenAnh/References/khoa-sqlite-driver-benchmark/data/zombie.db")
	defer zombieMainPool.CloseAll()

	// === Modernc (modernc.org/sqlite) ===
	moderncPool := modernc.SetupModerncPool(ctx, "C:/Users/KhoaNguyenAnh/References/khoa-sqlite-driver-benchmark/data/modernc.db")
	defer moderncPool.CloseAll()

	fmt.Println("Both drivers initialized successfully — ready for benchmarking.")
}
