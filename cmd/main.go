package main

import (
	"context"
	"fmt"
	"khoa-sqlite-driver-benchmark/zombie"
)

func main() {
	ctx := context.Background()

	// === DB ===
	zombieMainPool := zombie.SetupMainPool(ctx, "C:/Users/KhoaNguyenAnh/References/khoa-sqlite-driver-benchmark/data/zombie.db")
	defer zombieMainPool.CloseAll()

	fmt.Println("hello")
}
