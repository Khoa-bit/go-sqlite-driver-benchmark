package modernc

import (
	"database/sql"
	"fmt"
)

// SetPragma applies production-grade SQLite PRAGMAs to the given *sql.DB.
// These match the same settings used by the zombie driver for fair comparison.
// Best practice for modernc is to set PRAGMAs via DSN _pragma parameters,
// but we also apply them explicitly here as a safety net and for clarity.
func SetPragma(db *sql.DB) error {
	pragmas := []struct {
		query string
	}{
		{"PRAGMA journal_mode  = WAL"},
		{"PRAGMA busy_timeout  = 5000"},   // 5 seconds
		{"PRAGMA synchronous   = NORMAL"}, // safe with WAL
		{"PRAGMA foreign_keys  = ON"},
		{"PRAGMA temp_store    = MEMORY"},
		{"PRAGMA cache_size    = -65536"},    // 256 MB (negative = KB)
		{"PRAGMA mmap_size     = 268435456"}, // 256 MB
	}

	for _, p := range pragmas {
		if _, err := db.Exec(p.query); err != nil {
			return fmt.Errorf("failed to set %s: %w", p.query, err)
		}
	}

	return nil
}
