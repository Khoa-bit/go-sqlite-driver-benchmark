package benchmark

import (
	"context"
	"fmt"
	"khoa-sqlite-driver-benchmark/modernc"
	"khoa-sqlite-driver-benchmark/zombie"
	"sync"
	"time"

	"zombiezen.com/go/sqlite/sqlitex"
)

// Driver defines the operations that every benchmarked SQLite driver
// must implement. Each method mirrors a specific benchmark scenario.
type Driver interface {
	Name() string

	// Insert inserts a single row and returns the new primary key.
	Insert(ctx context.Context, name, email string, age int, balance float64) (int64, error)

	// BatchInsert inserts many rows inside a single transaction.
	BatchInsert(ctx context.Context, rows []UserRow) error

	// SelectByPK returns a row by its primary key.
	SelectByPK(ctx context.Context, id int64) (UserRow, error)

	// SelectByAgeRange returns all rows whose age falls within [minAge, maxAge].
	SelectByAgeRange(ctx context.Context, minAge, maxAge int) ([]UserRow, error)

	// UpdateEmail updates the email address of a single row.
	UpdateEmail(ctx context.Context, id int64, newEmail string) error

	// Delete removes a single row by primary key.
	Delete(ctx context.Context, id int64) error

	// ConcurrentReads runs N goroutines each doing SelectByPK on random IDs.
	ConcurrentReads(ctx context.Context, ids []int64, concurrency int) error
}

// ────────────────────────────  Zombie  ────────────────────────────

// ZombieDriver implements Driver using the zombiezen SQLite driver.
type ZombieDriver struct {
	pool zombie.MainPool
}

// NewZombieDriver creates a Driver backed by a zombie.MainPool.
func NewZombieDriver(pool zombie.MainPool) Driver {
	return &ZombieDriver{pool: pool}
}

func (d *ZombieDriver) Name() string { return "zombie" }

func (d *ZombieDriver) Insert(ctx context.Context, name, email string, age int, balance float64) (int64, error) {
	wc, done := d.pool.TakeWriteConn(ctx)
	defer done()

	now := time.Now().UTC().Format(time.RFC3339)
	err := sqlitex.Execute(wc.Conn,
		`INSERT INTO benchmark_users (name, email, age, balance, is_active, created_at, updated_at)
		 VALUES (?, ?, ?, ?, 1, ?, ?)`,
		&sqlitex.ExecOptions{
			Args: []any{name, email, age, balance, now, now},
		})
	if err != nil {
		return 0, err
	}
	return wc.Conn.LastInsertRowID(), nil
}

func (d *ZombieDriver) BatchInsert(ctx context.Context, rows []UserRow) error {
	wc, done := d.pool.TakeWriteConn(ctx)
	defer done()

	err := sqlitex.Execute(wc.Conn, "BEGIN IMMEDIATE", nil)
	if err != nil {
		return fmt.Errorf("batch begin: %w", err)
	}

	insStmt, err := wc.Conn.Prepare(
		`INSERT INTO benchmark_users (name, email, age, balance, is_active, created_at, updated_at)
		 VALUES (?, ?, ?, ?, 1, ?, ?)`)
	if err != nil {
		return fmt.Errorf("batch prepare: %w", err)
	}
	defer insStmt.Finalize()

	for i := range rows {
		insStmt.Reset()
		insStmt.BindText(1, rows[i].Name)
		insStmt.BindText(2, rows[i].Email)
		insStmt.BindInt64(3, int64(rows[i].Age))
		insStmt.BindFloat(4, rows[i].Balance)
		insStmt.BindText(5, rows[i].CreatedAt)
		insStmt.BindText(6, rows[i].UpdatedAt)
		if _, err := insStmt.Step(); err != nil {
			return fmt.Errorf("batch insert %d: %w", i, err)
		}
	}

	if err := sqlitex.Execute(wc.Conn, "COMMIT", nil); err != nil {
		return fmt.Errorf("batch commit: %w", err)
	}
	return nil
}

func (d *ZombieDriver) SelectByPK(ctx context.Context, id int64) (UserRow, error) {
	rc, done := d.pool.TakeReadConn(ctx)
	defer done()

	stmt, err := rc.Conn.Prepare("SELECT id, name, email, age, balance, created_at, updated_at FROM benchmark_users WHERE id = ?")
	if err != nil {
		return UserRow{}, err
	}
	defer stmt.Finalize()

	stmt.BindInt64(1, id)
	hasRow, err := stmt.Step()
	if err != nil {
		return UserRow{}, err
	}
	if !hasRow {
		return UserRow{}, fmt.Errorf("user %d not found", id)
	}

	return UserRow{
		ID:        stmt.ColumnInt64(0),
		Name:      stmt.ColumnText(1),
		Email:     stmt.ColumnText(2),
		Age:       int(stmt.ColumnInt64(3)),
		Balance:   stmt.ColumnFloat(4),
		CreatedAt: stmt.ColumnText(5),
		UpdatedAt: stmt.ColumnText(6),
	}, nil
}

func (d *ZombieDriver) SelectByAgeRange(ctx context.Context, minAge, maxAge int) ([]UserRow, error) {
	rc, done := d.pool.TakeReadConn(ctx)
	defer done()

	stmt, err := rc.Conn.Prepare("SELECT id, name, email, age, balance, created_at, updated_at FROM benchmark_users WHERE age BETWEEN ? AND ?")
	if err != nil {
		return nil, err
	}
	defer stmt.Finalize()

	stmt.BindInt64(1, int64(minAge))
	stmt.BindInt64(2, int64(maxAge))

	var rows []UserRow
	for {
		hasRow, err := stmt.Step()
		if err != nil {
			return nil, err
		}
		if !hasRow {
			break
		}
		rows = append(rows, UserRow{
			ID:        stmt.ColumnInt64(0),
			Name:      stmt.ColumnText(1),
			Email:     stmt.ColumnText(2),
			Age:       int(stmt.ColumnInt64(3)),
			Balance:   stmt.ColumnFloat(4),
			CreatedAt: stmt.ColumnText(5),
			UpdatedAt: stmt.ColumnText(6),
		})
	}
	return rows, nil
}

func (d *ZombieDriver) UpdateEmail(ctx context.Context, id int64, newEmail string) error {
	wc, done := d.pool.TakeWriteConn(ctx)
	defer done()

	return sqlitex.Execute(wc.Conn,
		`UPDATE benchmark_users SET email = ?, updated_at = ? WHERE id = ?`,
		&sqlitex.ExecOptions{
			Args: []any{newEmail, time.Now().UTC().Format(time.RFC3339), id},
		})
}

func (d *ZombieDriver) Delete(ctx context.Context, id int64) error {
	wc, done := d.pool.TakeWriteConn(ctx)
	defer done()

	return sqlitex.Execute(wc.Conn,
		"DELETE FROM benchmark_users WHERE id = ?",
		&sqlitex.ExecOptions{Args: []any{id}})
}

func (d *ZombieDriver) ConcurrentReads(ctx context.Context, ids []int64, concurrency int) error {
	var wg sync.WaitGroup
	errCh := make(chan error, concurrency)
	idCh := make(chan int64, len(ids))

	for _, id := range ids {
		idCh <- id
	}
	close(idCh)

	for i := 0; i < concurrency; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for id := range idCh {
				if _, err := d.SelectByPK(ctx, id); err != nil {
					select {
					case errCh <- err:
					default:
					}
				}
			}
		}()
	}
	wg.Wait()
	close(errCh)

	if err, ok := <-errCh; ok {
		return err
	}
	return nil
}

// ────────────────────────────  Modernc  ────────────────────────────

// ModerncDriver implements Driver using the modernc SQLite driver.
type ModerncDriver struct {
	pool modernc.ModerncPool
}

// NewModerncDriver creates a Driver backed by a modernc.ModerncPool.
func NewModerncDriver(pool modernc.ModerncPool) Driver {
	return &ModerncDriver{pool: pool}
}

func (d *ModerncDriver) Name() string { return "modernc" }

func (d *ModerncDriver) Insert(ctx context.Context, name, email string, age int, balance float64) (int64, error) {
	now := time.Now().UTC().Format(time.RFC3339)
	res, err := d.pool.WriteDB.ExecContext(ctx,
		`INSERT INTO benchmark_users (name, email, age, balance, is_active, created_at, updated_at)
		 VALUES (?, ?, ?, ?, 1, ?, ?)`,
		name, email, age, balance, now, now)
	if err != nil {
		return 0, err
	}
	return res.LastInsertId()
}

func (d *ModerncDriver) BatchInsert(ctx context.Context, rows []UserRow) error {
	tx, err := d.pool.WriteDB.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("batch begin: %w", err)
	}
	defer tx.Rollback()

	stmt, err := tx.PrepareContext(ctx,
		`INSERT INTO benchmark_users (name, email, age, balance, is_active, created_at, updated_at)
		 VALUES (?, ?, ?, ?, 1, ?, ?)`)
	if err != nil {
		return fmt.Errorf("batch prepare: %w", err)
	}
	defer stmt.Close()

	for i := range rows {
		if _, err := stmt.ExecContext(ctx, rows[i].Name, rows[i].Email, rows[i].Age, rows[i].Balance, rows[i].CreatedAt, rows[i].UpdatedAt); err != nil {
			return fmt.Errorf("batch insert %d: %w", i, err)
		}
	}

	return tx.Commit()
}

func (d *ModerncDriver) SelectByPK(ctx context.Context, id int64) (UserRow, error) {
	row := d.pool.ReadDB.QueryRowContext(ctx,
		"SELECT id, name, email, age, balance, created_at, updated_at FROM benchmark_users WHERE id = ?", id)
	var r UserRow
	err := row.Scan(&r.ID, &r.Name, &r.Email, &r.Age, &r.Balance, &r.CreatedAt, &r.UpdatedAt)
	return r, err
}

func (d *ModerncDriver) SelectByAgeRange(ctx context.Context, minAge, maxAge int) ([]UserRow, error) {
	rows, err := d.pool.ReadDB.QueryContext(ctx,
		"SELECT id, name, email, age, balance, created_at, updated_at FROM benchmark_users WHERE age BETWEEN ? AND ?",
		minAge, maxAge)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var result []UserRow
	for rows.Next() {
		var r UserRow
		if err := rows.Scan(&r.ID, &r.Name, &r.Email, &r.Age, &r.Balance, &r.CreatedAt, &r.UpdatedAt); err != nil {
			return nil, err
		}
		result = append(result, r)
	}
	return result, rows.Err()
}

func (d *ModerncDriver) UpdateEmail(ctx context.Context, id int64, newEmail string) error {
	_, err := d.pool.WriteDB.ExecContext(ctx,
		"UPDATE benchmark_users SET email = ?, updated_at = ? WHERE id = ?",
		newEmail, time.Now().UTC().Format(time.RFC3339), id)
	return err
}

func (d *ModerncDriver) Delete(ctx context.Context, id int64) error {
	_, err := d.pool.WriteDB.ExecContext(ctx,
		"DELETE FROM benchmark_users WHERE id = ?", id)
	return err
}

func (d *ModerncDriver) ConcurrentReads(ctx context.Context, ids []int64, concurrency int) error {
	var wg sync.WaitGroup
	errCh := make(chan error, concurrency)
	idCh := make(chan int64, len(ids))

	for _, id := range ids {
		idCh <- id
	}
	close(idCh)

	for i := 0; i < concurrency; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for id := range idCh {
				if _, err := d.SelectByPK(ctx, id); err != nil {
					select {
					case errCh <- err:
					default:
					}
				}
			}
		}()
	}
	wg.Wait()
	close(errCh)

	if err, ok := <-errCh; ok {
		return err
	}
	return nil
}
