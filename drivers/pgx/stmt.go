package pgx

import (
	"context"
	"database/sql"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/stephenafamo/scan"
)

// Prepared is a prepared statement on a pgx connection.
// It implements [bob.PreparedExecutor].
//
// The SQL is used as both the statement name and query (matching pgx/stdlib),
// so later Exec/Query calls reuse the prepared statement.
type Prepared struct {
	sql       string
	conn      *pgx.Conn
	tx        pgx.Tx // when set, Exec/Query run on the transaction
	stickyErr error  // if set, returned by Exec/Query/Close (like database/sql)
	noopClose bool   // true for StmtContext results; parent owns deallocation
}

// PrepareContext creates a prepared statement for later queries or executions
func (c Conn) PrepareContext(ctx context.Context, query string) (Prepared, error) {
	_, err := c.Conn.Prepare(ctx, query, query)
	if err != nil {
		return Prepared{}, err
	}

	return Prepared{sql: query, conn: c.Conn}, nil
}

// PrepareContext creates a prepared statement for later queries or executions
func (c PoolConn) PrepareContext(ctx context.Context, query string) (Prepared, error) {
	conn := c.Conn.Conn()
	_, err := conn.Prepare(ctx, query, query)
	if err != nil {
		return Prepared{}, err
	}

	return Prepared{sql: query, conn: conn}, nil
}

// PrepareContext creates a prepared statement for later queries or executions
func (t Tx) PrepareContext(ctx context.Context, query string) (Prepared, error) {
	_, err := t.Prepare(ctx, query, query)
	if err != nil {
		return Prepared{}, err
	}

	return Prepared{sql: query, conn: t.Conn(), tx: t.tx}, nil
}

// StmtContext returns a prepared statement that will be executed on this transaction.
// The statement is re-prepared on the transaction's connection when needed
// (idempotent; matching database/sql Tx.StmtContext). See [bob.InTx].
func (t Tx) StmtContext(ctx context.Context, stmt Prepared) Prepared {
	if stmt.stickyErr != nil {
		return Prepared{stickyErr: stmt.stickyErr}
	}

	_, err := t.Prepare(ctx, stmt.sql, stmt.sql)
	if err != nil {
		return Prepared{stickyErr: err}
	}

	return Prepared{sql: stmt.sql, conn: t.Conn(), tx: t.tx, noopClose: true}
}

// ExecContext executes a prepared statement without returning any rows
func (s Prepared) ExecContext(ctx context.Context, args ...any) (sql.Result, error) {
	if s.stickyErr != nil {
		return nil, s.stickyErr
	}

	if s.tx != nil {
		tag, err := s.tx.Exec(ctx, s.sql, args...)
		return result{tag}, err
	}

	tag, err := s.conn.Exec(ctx, s.sql, args...)
	return result{tag}, err
}

// QueryContext executes a prepared statement that returns rows
func (s Prepared) QueryContext(ctx context.Context, args ...any) (scan.Rows, error) {
	if s.stickyErr != nil {
		return nil, s.stickyErr
	}

	if s.tx != nil {
		pgxRows, err := s.tx.Query(ctx, s.sql, args...)
		return rows{pgxRows}, err
	}

	pgxRows, err := s.conn.Query(ctx, s.sql, args...)
	return rows{pgxRows}, err
}

// Close deallocates the prepared statement on the server.
// Statements returned by [Tx.StmtContext] are a no-op on Close; the parent
// statement owns deallocation (matching database/sql).
func (s Prepared) Close() error {
	if s.stickyErr != nil {
		return s.stickyErr
	}
	if s.noopClose {
		return nil
	}

	ctx, cancel := context.WithTimeout(context.Background(), time.Second*5)
	defer cancel()

	return s.conn.Deallocate(ctx, s.sql)
}
