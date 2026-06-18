package benchmark

import (
	"context"
	"fmt"
	"khoa-sqlite-driver-benchmark/modernc"
	"khoa-sqlite-driver-benchmark/zombie"
	"sync"
	"time"

	"zombiezen.com/go/sqlite"
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

	// ContentionWriteRead runs multiple readers concurrently with a single writer.
	// readerCount goroutines each execute opsPerReader SelectByPK calls,
	// while 1 writer goroutine executes writerOps Insert calls.
	ContentionWriteRead(ctx context.Context, ids []int64, readerCount, opsPerReader, writerOps int) error

	// TxWriteOnly executes a pure-write transaction: INSERT → UPDATE → DELETE on the same row.
	TxWriteOnly(ctx context.Context) error

	// TxReadWrite executes a mixed read+write transaction: SELECT by PK → UPDATE email.
	TxReadWrite(ctx context.Context, id int64, newEmail string) error
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

func (d *ZombieDriver) BatchInsert(ctx context.Context, rows []UserRow) (err error) {
	wc, done := d.pool.TakeWriteConn(ctx)
	defer done()
	defer sqlitex.Save(wc.Conn)(&err)

	for i := range rows {
		err = sqlitex.Execute(wc.Conn,
			`INSERT INTO benchmark_users (name, email, age, balance, is_active, created_at, updated_at)
			 VALUES (?, ?, ?, ?, 1, ?, ?)`,
			&sqlitex.ExecOptions{
				Args: []any{rows[i].Name, rows[i].Email, rows[i].Age, rows[i].Balance, rows[i].CreatedAt, rows[i].UpdatedAt},
			})
		if err != nil {
			return fmt.Errorf("batch insert %d: %w", i, err)
		}
	}

	return nil
}

func (d *ZombieDriver) SelectByPK(ctx context.Context, id int64) (UserRow, error) {
	rc, done := d.pool.TakeReadConn(ctx)
	defer done()

	var row UserRow
	found := false
	err := sqlitex.Execute(rc.Conn,
		"SELECT id, name, email, age, balance, created_at, updated_at FROM benchmark_users WHERE id = ?",
		&sqlitex.ExecOptions{
			Args: []any{id},
			ResultFunc: func(stmt *sqlite.Stmt) error {
				found = true
				row = UserRow{
					ID:        stmt.ColumnInt64(0),
					Name:      stmt.ColumnText(1),
					Email:     stmt.ColumnText(2),
					Age:       int(stmt.ColumnInt64(3)),
					Balance:   stmt.ColumnFloat(4),
					CreatedAt: stmt.ColumnText(5),
					UpdatedAt: stmt.ColumnText(6),
				}
				return nil
			},
		})
	if err != nil {
		return UserRow{}, err
	}
	if !found {
		return UserRow{}, fmt.Errorf("user %d not found", id)
	}
	return row, nil
}

func (d *ZombieDriver) SelectByAgeRange(ctx context.Context, minAge, maxAge int) ([]UserRow, error) {
	rc, done := d.pool.TakeReadConn(ctx)
	defer done()

	var rows []UserRow
	err := sqlitex.Execute(rc.Conn,
		"SELECT id, name, email, age, balance, created_at, updated_at FROM benchmark_users WHERE age BETWEEN ? AND ?",
		&sqlitex.ExecOptions{
			Args: []any{minAge, maxAge},
			ResultFunc: func(stmt *sqlite.Stmt) error {
				rows = append(rows, UserRow{
					ID:        stmt.ColumnInt64(0),
					Name:      stmt.ColumnText(1),
					Email:     stmt.ColumnText(2),
					Age:       int(stmt.ColumnInt64(3)),
					Balance:   stmt.ColumnFloat(4),
					CreatedAt: stmt.ColumnText(5),
					UpdatedAt: stmt.ColumnText(6),
				})
				return nil
			},
		})
	return rows, err
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

func (d *ZombieDriver) ContentionWriteRead(ctx context.Context, ids []int64, readerCount, opsPerReader, writerOps int) error {
	var wg sync.WaitGroup
	errCh := make(chan error, readerCount+1)

	// Launch reader goroutines.
	for i := 0; i < readerCount; i++ {
		i := i
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := 0; j < opsPerReader; j++ {
				id := ids[(i*opsPerReader+j)%len(ids)]
				if _, err := d.SelectByPK(ctx, id); err != nil {
					select {
					case errCh <- err:
					default:
					}
					return
				}
			}
		}()
	}

	// Launch 1 writer goroutine.
	wg.Add(1)
	go func() {
		defer wg.Done()
		for k := 0; k < writerOps; k++ {
			_, err := d.Insert(ctx,
				fmt.Sprintf("contend_w_%d", k),
				fmt.Sprintf("contend_w_%d@example.com", k),
				30, float64(k),
			)
			if err != nil {
				select {
				case errCh <- err:
				default:
				}
				return
			}
		}
	}()

	wg.Wait()
	close(errCh)

	if err, ok := <-errCh; ok {
		return err
	}
	return nil
}

func (d *ZombieDriver) TxWriteOnly(ctx context.Context) (err error) {
	wc, done := d.pool.TakeWriteConn(ctx)
	defer done()
	defer sqlitex.Save(wc.Conn)(&err)

	now := time.Now().UTC().Format(time.RFC3339)

	// INSERT
	err = sqlitex.Execute(wc.Conn,
		`INSERT INTO benchmark_users (name, email, age, balance, is_active, created_at, updated_at)
		 VALUES (?, ?, ?, ?, 1, ?, ?)`,
		&sqlitex.ExecOptions{
			Args: []any{"tx_write_only", "tx_wo@example.com", 30, 100.0, now, now},
		})
	if err != nil {
		return err
	}
	id := wc.Conn.LastInsertRowID()

	// UPDATE
	err = sqlitex.Execute(wc.Conn,
		"UPDATE benchmark_users SET email = ?, updated_at = ? WHERE id = ?",
		&sqlitex.ExecOptions{
			Args: []any{"tx_wo_updated@example.com", now, id},
		})
	if err != nil {
		return err
	}

	// DELETE
	return sqlitex.Execute(wc.Conn,
		"DELETE FROM benchmark_users WHERE id = ?",
		&sqlitex.ExecOptions{Args: []any{id}})
}

func (d *ZombieDriver) TxReadWrite(ctx context.Context, id int64, newEmail string) (err error) {
	wc, done := d.pool.TakeWriteConn(ctx)
	defer done()
	defer sqlitex.Save(wc.Conn)(&err)

	now := time.Now().UTC().Format(time.RFC3339)

	// SELECT
	var found bool
	err = sqlitex.Execute(wc.Conn,
		"SELECT id FROM benchmark_users WHERE id = ?",
		&sqlitex.ExecOptions{
			Args: []any{id},
			ResultFunc: func(stmt *sqlite.Stmt) error {
				found = true
				return nil
			},
		})
	if err != nil {
		return err
	}
	if !found {
		return fmt.Errorf("user %d not found", id)
	}

	// UPDATE
	return sqlitex.Execute(wc.Conn,
		"UPDATE benchmark_users SET email = ?, updated_at = ? WHERE id = ?",
		&sqlitex.ExecOptions{
			Args: []any{newEmail, now, id},
		})
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
	stmt, err := d.pool.WriteDB.PrepareContext(ctx,
		`INSERT INTO benchmark_users (name, email, age, balance, is_active, created_at, updated_at)
		 VALUES (?, ?, ?, ?, 1, ?, ?)`)
	if err != nil {
		return 0, err
	}
	defer stmt.Close()
	res, err := stmt.ExecContext(ctx, name, email, age, balance, now, now)
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
	stmt, err := d.pool.ReadDB.PrepareContext(ctx,
		"SELECT id, name, email, age, balance, created_at, updated_at FROM benchmark_users WHERE id = ?")
	if err != nil {
		return UserRow{}, err
	}
	defer stmt.Close()
	row := stmt.QueryRowContext(ctx, id)
	var r UserRow
	err = row.Scan(&r.ID, &r.Name, &r.Email, &r.Age, &r.Balance, &r.CreatedAt, &r.UpdatedAt)
	return r, err
}

func (d *ModerncDriver) SelectByAgeRange(ctx context.Context, minAge, maxAge int) ([]UserRow, error) {
	stmt, err := d.pool.ReadDB.PrepareContext(ctx,
		"SELECT id, name, email, age, balance, created_at, updated_at FROM benchmark_users WHERE age BETWEEN ? AND ?")
	if err != nil {
		return nil, err
	}
	defer stmt.Close()
	rows, err := stmt.QueryContext(ctx, minAge, maxAge)
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
	stmt, err := d.pool.WriteDB.PrepareContext(ctx,
		"UPDATE benchmark_users SET email = ?, updated_at = ? WHERE id = ?")
	if err != nil {
		return err
	}
	defer stmt.Close()
	_, err = stmt.ExecContext(ctx, newEmail, time.Now().UTC().Format(time.RFC3339), id)
	return err
}

func (d *ModerncDriver) Delete(ctx context.Context, id int64) error {
	stmt, err := d.pool.WriteDB.PrepareContext(ctx,
		"DELETE FROM benchmark_users WHERE id = ?")
	if err != nil {
		return err
	}
	defer stmt.Close()
	_, err = stmt.ExecContext(ctx, id)
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

func (d *ModerncDriver) ContentionWriteRead(ctx context.Context, ids []int64, readerCount, opsPerReader, writerOps int) error {
	var wg sync.WaitGroup
	errCh := make(chan error, readerCount+1)

	// Launch reader goroutines.
	for i := 0; i < readerCount; i++ {
		i := i
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := 0; j < opsPerReader; j++ {
				id := ids[(i*opsPerReader+j)%len(ids)]
				if _, err := d.SelectByPK(ctx, id); err != nil {
					select {
					case errCh <- err:
					default:
					}
					return
				}
			}
		}()
	}

	// Launch 1 writer goroutine.
	wg.Add(1)
	go func() {
		defer wg.Done()
		for k := 0; k < writerOps; k++ {
			_, err := d.Insert(ctx,
				fmt.Sprintf("contend_w_%d", k),
				fmt.Sprintf("contend_w_%d@example.com", k),
				30, float64(k),
			)
			if err != nil {
				select {
				case errCh <- err:
				default:
				}
				return
			}
		}
	}()

	wg.Wait()
	close(errCh)

	if err, ok := <-errCh; ok {
		return err
	}
	return nil
}

func (d *ModerncDriver) TxWriteOnly(ctx context.Context) error {
	tx, err := d.pool.WriteDB.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("tx write-only begin: %w", err)
	}
	defer tx.Rollback()

	now := time.Now().UTC().Format(time.RFC3339)

	// INSERT
	res, err := tx.ExecContext(ctx,
		`INSERT INTO benchmark_users (name, email, age, balance, is_active, created_at, updated_at)
		 VALUES (?, ?, ?, ?, 1, ?, ?)`,
		"tx_write_only", "tx_wo@example.com", 30, 100.0, now, now)
	if err != nil {
		return fmt.Errorf("tx write-only insert: %w", err)
	}
	id, _ := res.LastInsertId()

	// UPDATE
	_, err = tx.ExecContext(ctx,
		"UPDATE benchmark_users SET email = ?, updated_at = ? WHERE id = ?",
		"tx_wo_updated@example.com", now, id)
	if err != nil {
		return fmt.Errorf("tx write-only update: %w", err)
	}

	// DELETE
	_, err = tx.ExecContext(ctx,
		"DELETE FROM benchmark_users WHERE id = ?", id)
	if err != nil {
		return fmt.Errorf("tx write-only delete: %w", err)
	}

	return tx.Commit()
}

func (d *ModerncDriver) TxReadWrite(ctx context.Context, id int64, newEmail string) error {
	tx, err := d.pool.WriteDB.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("tx read-write begin: %w", err)
	}
	defer tx.Rollback()

	now := time.Now().UTC().Format(time.RFC3339)

	// SELECT
	var foundID int64
	err = tx.QueryRowContext(ctx,
		"SELECT id FROM benchmark_users WHERE id = ?", id).Scan(&foundID)
	if err != nil {
		return fmt.Errorf("tx read-write select: %w", err)
	}

	// UPDATE
	_, err = tx.ExecContext(ctx,
		"UPDATE benchmark_users SET email = ?, updated_at = ? WHERE id = ?",
		newEmail, now, id)
	if err != nil {
		return fmt.Errorf("tx read-write update: %w", err)
	}

	return tx.Commit()
}
