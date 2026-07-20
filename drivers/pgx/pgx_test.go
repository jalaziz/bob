package pgx

import (
	"context"
	"database/sql"

	"github.com/jackc/pgx/v5"
	"github.com/stephenafamo/bob"
	"github.com/stephenafamo/scan"
)

var (
	_ sql.Result = result{}
	_ scan.Rows  = rows{}
)

var (
	_ bob.Executor       = Pool{}
	_ bob.Transactor[Tx] = Pool{}
)

var (
	_ bob.Executor           = Conn{}
	_ bob.Transactor[Tx]     = Conn{}
	_ bob.Preparer[Prepared] = Conn{}
)

var (
	_ bob.Executor           = PoolConn{}
	_ bob.Preparer[Prepared] = PoolConn{}
)

var (
	_ bob.Executor           = Tx{}
	_ bob.Transaction        = Tx{}
	_ bob.Preparer[Prepared] = Tx{}
)

var _ bob.PreparedExecutor = Prepared{}

var _ interface {
	StmtContext(context.Context, Prepared) Prepared
} = Tx{}

// Ensure that our Tx satisfies pgx.Tx interface
// we wrap the given pgx.Tx, to add auto rollback based on context cancellation
// so we do not simply embed it
var _ pgx.Tx = Tx{}
