package pgx

import (
	"context"
	"io"
	"log"
	"testing"

	"github.com/stephenafamo/bob"
	"github.com/stephenafamo/bob/dialect/psql"
	"github.com/stephenafamo/bob/dialect/psql/im"
	"github.com/stephenafamo/bob/dialect/psql/sm"
	"github.com/stephenafamo/scan"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/modules/postgres"
)

func startPostgres(t *testing.T) string {
	t.Helper()

	container, err := postgres.Run(
		t.Context(),
		"postgres:16-alpine",
		postgres.BasicWaitStrategies(),
		testcontainers.WithLogger(log.New(io.Discard, "", log.LstdFlags)),
	)
	if err != nil {
		t.Skipf("could not start postgres container: %v", err)
	}
	t.Cleanup(func() {
		if err := testcontainers.TerminateContainer(container); err != nil {
			t.Logf("failed to terminate container: %v", err)
		}
	})

	dsn, err := container.ConnectionString(t.Context(), "sslmode=disable")
	if err != nil {
		t.Fatalf("connection string: %v", err)
	}
	return dsn
}

func TestPrepared(t *testing.T) {
	ctx := t.Context()
	pool, err := New(ctx, startPostgres(t))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(pool.Close)

	t.Run("conn exec query close", func(t *testing.T) {
		conn, err := pool.Acquire(ctx)
		if err != nil {
			t.Fatal(err)
		}
		defer conn.Release()

		const query = "SELECT $1::int"
		stmt, err := conn.PrepareContext(ctx, query)
		if err != nil {
			t.Fatal(err)
		}

		got, err := queryInt(ctx, stmt, 7)
		if err != nil {
			t.Fatal(err)
		}
		if got != 7 {
			t.Fatalf("got %d, want 7", got)
		}

		if _, err := stmt.ExecContext(ctx, 8); err != nil {
			t.Fatal(err)
		}

		if err := stmt.Close(); err != nil {
			t.Fatal(err)
		}

		// Re-prepare after Close must succeed (statement was deallocated).
		stmt2, err := conn.PrepareContext(ctx, query)
		if err != nil {
			t.Fatal(err)
		}
		if err := stmt2.Close(); err != nil {
			t.Fatal(err)
		}
	})

	t.Run("poolConn", func(t *testing.T) {
		conn, err := pool.Acquire(ctx)
		if err != nil {
			t.Fatal(err)
		}
		defer conn.Release()

		stmt, err := conn.PrepareContext(ctx, "SELECT $1::text")
		if err != nil {
			t.Fatal(err)
		}
		defer stmt.Close()

		rows, err := stmt.QueryContext(ctx, "hello")
		if err != nil {
			t.Fatal(err)
		}
		defer rows.Close()

		if !rows.Next() {
			t.Fatal("expected a row")
		}
		var s string
		if err := rows.Scan(&s); err != nil {
			t.Fatal(err)
		}
		if s != "hello" {
			t.Fatalf("got %q, want %q", s, "hello")
		}
	})

	t.Run("tx", func(t *testing.T) {
		tx, err := pool.Begin(ctx)
		if err != nil {
			t.Fatal(err)
		}
		defer tx.Rollback(ctx)

		stmt, err := tx.PrepareContext(ctx, "SELECT $1::int * 2")
		if err != nil {
			t.Fatal(err)
		}
		defer stmt.Close()

		got, err := queryInt(ctx, stmt, 21)
		if err != nil {
			t.Fatal(err)
		}
		if got != 42 {
			t.Fatalf("got %d, want 42", got)
		}
	})

	t.Run("stmtContext re-prepare and noop close", func(t *testing.T) {
		conn, err := pool.Acquire(ctx)
		if err != nil {
			t.Fatal(err)
		}
		defer conn.Release()

		parent, err := conn.PrepareContext(ctx, "SELECT $1::int")
		if err != nil {
			t.Fatal(err)
		}
		defer parent.Close()

		tx, err := conn.Begin(ctx)
		if err != nil {
			t.Fatal(err)
		}

		txStmt := tx.StmtContext(ctx, parent)
		if _, err := queryInt(ctx, txStmt, 3); err != nil {
			t.Fatal(err)
		}

		// Closing the tx-bound statement must not deallocate the parent.
		if err := txStmt.Close(); err != nil {
			t.Fatal(err)
		}
		if err := tx.Commit(ctx); err != nil {
			t.Fatal(err)
		}

		got, err := queryInt(ctx, parent, 4)
		if err != nil {
			t.Fatal(err)
		}
		if got != 4 {
			t.Fatalf("got %d, want 4", got)
		}
	})

	t.Run("stmtContext cross connection", func(t *testing.T) {
		connA, err := pool.Acquire(ctx)
		if err != nil {
			t.Fatal(err)
		}
		defer connA.Release()

		connB, err := pool.Acquire(ctx)
		if err != nil {
			t.Fatal(err)
		}
		defer connB.Release()

		parent, err := connA.PrepareContext(ctx, "SELECT $1::int")
		if err != nil {
			t.Fatal(err)
		}
		defer parent.Close()

		tx, err := connB.Begin(ctx)
		if err != nil {
			t.Fatal(err)
		}
		defer tx.Rollback(ctx)

		txStmt := tx.StmtContext(ctx, parent)
		if txStmt.stickyErr != nil {
			t.Fatalf("re-prepare on tx connection: %v", txStmt.stickyErr)
		}

		got, err := queryInt(ctx, txStmt, 9)
		if err != nil {
			t.Fatal(err)
		}
		if got != 9 {
			t.Fatalf("got %d, want 9", got)
		}
	})

	t.Run("bob PrepareQuery and InTx", func(t *testing.T) {
		conn, err := pool.Acquire(ctx)
		if err != nil {
			t.Fatal(err)
		}
		defer conn.Release()

		selectQ := psql.Select(sm.Columns(psql.Cast(bob.Named("n"), "int")))
		qstmt, err := bob.PrepareQuery[int](ctx, conn, selectQ, scan.SingleColumnMapper[int])
		if err != nil {
			t.Fatal(err)
		}
		defer qstmt.Close()

		got, err := qstmt.One(ctx, 11)
		if err != nil {
			t.Fatal(err)
		}
		if got != 11 {
			t.Fatalf("got %d, want 11", got)
		}

		all, err := qstmt.All(ctx, 12)
		if err != nil {
			t.Fatal(err)
		}
		if len(all) != 1 || all[0] != 12 {
			t.Fatalf("got %v, want [12]", all)
		}

		if _, err := conn.ExecContext(ctx, `CREATE TEMP TABLE items (id int primary key, n int)`); err != nil {
			t.Fatal(err)
		}

		type itemArgs struct {
			ID int `db:"id"`
			N  int `db:"n"`
		}
		insertQ := psql.Insert(
			im.Into("items", "id", "n"),
			im.Values(bob.Named("id"), bob.Named("n")),
		)
		insertStmt, err := bob.Prepare[itemArgs](ctx, conn, insertQ)
		if err != nil {
			t.Fatal(err)
		}
		defer insertStmt.Close()

		tx, err := conn.Begin(ctx)
		if err != nil {
			t.Fatal(err)
		}
		defer tx.Rollback(ctx)

		res, err := bob.InTx(ctx, insertStmt, tx).Exec(ctx, itemArgs{ID: 1, N: 10})
		if err != nil {
			t.Fatal(err)
		}
		affected, err := res.RowsAffected()
		if err != nil {
			t.Fatal(err)
		}
		if affected != 1 {
			t.Fatalf("rows affected: got %d, want 1", affected)
		}
		if err := tx.Commit(ctx); err != nil {
			t.Fatal(err)
		}

		var n int
		if err := conn.QueryRow(ctx, `SELECT n FROM items WHERE id = $1`, 1).Scan(&n); err != nil {
			t.Fatal(err)
		}
		if n != 10 {
			t.Fatalf("got n=%d, want 10", n)
		}
	})
}

func queryInt(ctx context.Context, stmt Prepared, arg int) (int, error) {
	rows, err := stmt.QueryContext(ctx, arg)
	if err != nil {
		return 0, err
	}
	defer rows.Close()

	if !rows.Next() {
		return 0, sqlNoRows()
	}
	var n int
	if err := rows.Scan(&n); err != nil {
		return 0, err
	}
	return n, rows.Err()
}

func sqlNoRows() error {
	return errNoRows{}
}

type errNoRows struct{}

func (errNoRows) Error() string { return "no rows" }
