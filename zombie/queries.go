package zombie

import (
	"context"
	"khoa-sqlite-driver-benchmark/tool"
	"runtime"

	"zombiezen.com/go/sqlite"
	"zombiezen.com/go/sqlite/sqlitex"
)

type MainPool struct {
	readPool  *sqlitex.Pool
	writePool *sqlitex.Pool
}

type MainReadConn struct {
	Ctx  context.Context
	Conn *sqlite.Conn
}

type MainWriteConn struct {
	MainReadConn
}

func (m *MainPool) TakeReadConn(ctx context.Context) (MainReadConn, func()) {
	conn, err := m.readPool.Take(ctx)
	tool.Assert(err == nil, "failed to take connection from pool", "error", err)
	return MainReadConn{Ctx: ctx, Conn: conn}, func() {
		m.readPool.Put(conn)
	}
}

func (m *MainPool) TakeWriteConn(ctx context.Context) (MainWriteConn, func()) {
	conn, err := m.writePool.Take(ctx)
	tool.Assert(err == nil, "failed to take connection from pool", "error", err)
	return MainWriteConn{MainReadConn: MainReadConn{Ctx: ctx, Conn: conn}}, func() {
		m.writePool.Put(conn)
	}
}

func (m *MainPool) CloseAll() {
	var err error
	err = m.readPool.Close()
	tool.Assert(err == nil, "failed to close readPool", "err", err)
	err = m.writePool.Close()
	tool.Assert(err == nil, "failed to close writePool", "err", err)
}

func SetupMainPool(ctx context.Context, dbName string) MainPool {
	// Open a writePool.
	writePool, err := sqlitex.NewPool("file:"+dbName, sqlitex.PoolOptions{
		Flags:       0,
		PoolSize:    1,
		PrepareConn: SetPragma,
	})
	tool.Assert(err == nil, "failed to open writePool", "error", err)

	// Open a readPool.
	readPool, err := sqlitex.NewPool("file:"+dbName+"?mode=ro", sqlitex.PoolOptions{
		Flags:       0,
		PoolSize:    runtime.NumCPU(),
		PrepareConn: SetPragma,
	})
	tool.Assert(err == nil, "failed to open readPool", "error", err)

	mainPool := MainPool{readPool: readPool, writePool: writePool}

	tool.Assert(err == nil, "failed to execute transaction", "error", err)

	return mainPool
}
