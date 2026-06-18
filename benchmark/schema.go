package benchmark

import (
	"database/sql"
	"fmt"
	"time"

	"zombiezen.com/go/sqlite"
	"zombiezen.com/go/sqlite/sqlitex"
)

// DDL is the schema used by every benchmark. It includes realistic columns
// and secondary indexes so that queries exercise more than just PK lookups.
const DDL = `
CREATE TABLE IF NOT EXISTS benchmark_users (
	id         INTEGER PRIMARY KEY AUTOINCREMENT,
	name       TEXT    NOT NULL,
	email      TEXT    NOT NULL,
	age        INTEGER NOT NULL,
	balance    REAL    NOT NULL DEFAULT 0.0,
	is_active  INTEGER NOT NULL DEFAULT 1,
	created_at TEXT    NOT NULL,
	updated_at TEXT    NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_users_email ON benchmark_users(email);
CREATE INDEX IF NOT EXISTS idx_users_age   ON benchmark_users(age);
`

// UserRow mirrors the benchmark_users table.
type UserRow struct {
	ID        int64
	Name      string
	Email     string
	Age       int
	Balance   float64
	CreatedAt string
	UpdatedAt string
}

// CreateSchemaZombie executes the DDL against a zombie *sqlite.Conn.
func CreateSchemaZombie(conn *sqlite.Conn) error {
	return sqlitex.ExecuteScript(conn, DDL, nil)
}

// CreateSchemaModernc executes the DDL against a modernc *sql.DB.
func CreateSchemaModernc(db *sql.DB) error {
	_, err := db.Exec(DDL)
	return err
}

// SeedDataZombie inserts n rows into benchmark_users via a zombie *sqlite.Conn.
// Uses sqlitex.Save for automatic BEGIN/COMMIT/ROLLBACK handling.
func SeedDataZombie(conn *sqlite.Conn, n int) (_ []int64, err error) {
	defer sqlitex.Save(conn)(&err)

	now := time.Now().UTC().Format(time.RFC3339)
	ids := make([]int64, 0, n)

	insStmt, err := conn.Prepare(
		`INSERT INTO benchmark_users (name, email, age, balance, is_active, created_at, updated_at)
		 VALUES (?, ?, ?, ?, 1, ?, ?)`)
	if err != nil {
		return nil, fmt.Errorf("seed prepare insert: %w", err)
	}
	defer insStmt.Finalize()

	for i := 0; i < n; i++ {
		name := fmt.Sprintf("user_%d", i)
		email := fmt.Sprintf("user_%d@example.com", i)
		age := 18 + (i % 60) // ages 18–77
		balance := float64(i%10000) * 1.5

		insStmt.Reset()
		insStmt.BindText(1, name)
		insStmt.BindText(2, email)
		insStmt.BindInt64(3, int64(age))
		insStmt.BindFloat(4, balance)
		insStmt.BindText(5, now)
		insStmt.BindText(6, now)

		hasRow, err := insStmt.Step()
		if err != nil {
			return nil, fmt.Errorf("seed insert %d: %w", i, err)
		}
		if hasRow {
			return nil, fmt.Errorf("seed insert %d: unexpected row from INSERT", i)
		}
		ids = append(ids, conn.LastInsertRowID())
	}

	return ids, nil
}

// SeedDataModernc inserts n rows via a modernc *sql.DB in a single transaction.
func SeedDataModernc(db *sql.DB, n int) ([]int64, error) {
	now := time.Now().UTC().Format(time.RFC3339)
	ids := make([]int64, 0, n)

	tx, err := db.Begin()
	if err != nil {
		return nil, fmt.Errorf("seed begin: %w", err)
	}
	defer tx.Rollback() // no-op after commit

	stmt, err := tx.Prepare(
		`INSERT INTO benchmark_users (name, email, age, balance, is_active, created_at, updated_at)
		 VALUES (?, ?, ?, ?, 1, ?, ?)`)
	if err != nil {
		return nil, fmt.Errorf("seed prepare insert: %w", err)
	}
	defer stmt.Close()

	for i := 0; i < n; i++ {
		name := fmt.Sprintf("user_%d", i)
		email := fmt.Sprintf("user_%d@example.com", i)
		age := 18 + (i % 60)
		balance := float64(i%10000) * 1.5

		res, err := stmt.Exec(name, email, age, balance, now, now)
		if err != nil {
			return nil, fmt.Errorf("seed insert %d: %w", i, err)
		}
		id, _ := res.LastInsertId()
		ids = append(ids, id)
	}

	if err := tx.Commit(); err != nil {
		return nil, fmt.Errorf("seed commit: %w", err)
	}

	return ids, nil
}
