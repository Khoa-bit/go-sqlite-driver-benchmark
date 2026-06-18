package modernc

import (
	"context"
	"database/sql"
	"khoa-sqlite-driver-benchmark/tool"
	"runtime"

	_ "modernc.org/sqlite"
)

// ModerncPool mirrors the zombie.MainPool architecture:
// a single-writer WriteDB for mutations and a multi-reader ReadDB for queries.
type ModerncPool struct {
	ReadDB  *sql.DB
	WriteDB *sql.DB
}

// SetupModerncPool opens two *sql.DB connections to the same SQLite file:
//   - WriteDB: single connection (SetMaxOpenConns=1), with WAL + performance PRAGMAs.
//   - ReadDB:  read-only, poolSize = runtime.NumCPU().
//
// PRAGMAs are set both via DSN _pragma parameters and via an explicit exec fallback.
func SetupModerncPool(ctx context.Context, dbPath string) ModerncPool {
	// Common PRAGMA suffix appended to every DSN.
	// See https://pkg.go.dev/modernc.org/sqlite#section-readme for _pragma syntax.
	const pragmaDSN = "_pragma=journal_mode(WAL)" +
		"&_pragma=busy_timeout(5000)" +
		"&_pragma=synchronous(NORMAL)" +
		"&_pragma=foreign_keys(ON)" +
		"&_pragma=temp_store(MEMORY)" +
		"&_pragma=cache_size(-65536)" +
		"&_pragma=mmap_size(268435456)"

	// --- Write DB ---
	writeDSN := "file:" + dbPath + "?" + pragmaDSN
	writeDB, err := sql.Open("sqlite", writeDSN)
	tool.Assert(err == nil, "failed to open modernc write DB", "error", err)
	writeDB.SetMaxOpenConns(1) // single writer for SQLite correctness

	if err := SetPragma(writeDB); err != nil {
		tool.Assert(false, "failed to set pragmas on write DB", "error", err)
	}

	// --- Read DB ---
	readDSN := "file:" + dbPath + "?mode=ro&" + pragmaDSN
	readDB, err := sql.Open("sqlite", readDSN)
	tool.Assert(err == nil, "failed to open modernc read DB", "error", err)
	readDB.SetMaxOpenConns(runtime.NumCPU())

	if err := SetPragma(readDB); err != nil {
		tool.Assert(false, "failed to set pragmas on read DB", "error", err)
	}

	_ = ctx // reserved for future use

	return ModerncPool{ReadDB: readDB, WriteDB: writeDB}
}

// CloseAll closes both read and write databases.
func (p *ModerncPool) CloseAll() {
	if err := p.ReadDB.Close(); err != nil {
		tool.Assert(false, "failed to close modernc read DB", "error", err)
	}
	if err := p.WriteDB.Close(); err != nil {
		tool.Assert(false, "failed to close modernc write DB", "error", err)
	}
}
