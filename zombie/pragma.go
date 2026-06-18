package zombie

import (
	"khoa-sqlite-driver-benchmark/tool"

	"zombiezen.com/go/sqlite"
	"zombiezen.com/go/sqlite/sqlitex"
)

func SetPragma(conn *sqlite.Conn) error {
	tool.Assert(sqlitex.ExecuteTransient(conn, "PRAGMA journal_mode  = WAL;", nil) == nil, "must be able to set PRAGMA")
	tool.Assert(sqlitex.ExecuteTransient(conn, "PRAGMA busy_timeout  = 5000;", nil) == nil, "must be able to set PRAGMA") // 5s
	tool.Assert(sqlitex.ExecuteTransient(conn, "PRAGMA synchronous   = NORMAL;", nil) == nil, "must be able to set PRAGMA")
	tool.Assert(sqlitex.ExecuteTransient(conn, "PRAGMA foreign_keys  = ON;", nil) == nil, "must be able to set PRAGMA")
	tool.Assert(sqlitex.ExecuteTransient(conn, "PRAGMA temp_store    = MEMORY;", nil) == nil, "must be able to set PRAGMA")
	tool.Assert(sqlitex.ExecuteTransient(conn, "PRAGMA cache_size    = 65536;", nil) == nil, "must be able to set PRAGMA")     // 256MB
	tool.Assert(sqlitex.ExecuteTransient(conn, "PRAGMA mmap_size     = 268435456;", nil) == nil, "must be able to set PRAGMA") // 256MB
	return nil
}
