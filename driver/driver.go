package driver

import (
	"context"

	"entgo.io/ent/dialect"
	"github.com/cockroachdb/cockroach-go/v2/crdb"
)

type (
	// Driver is a driver that adds support for cockroachdb.
	Driver struct {
		dialect.Driver // underlying driver.
	}
	// Tx is a transaction implementation that adds cockroachdb support.
	Tx struct {
		dialect.Tx                 // underlying transaction.
		ctx        context.Context // underlying transaction context.
	}
	// txAdapter implements crdb.Tx interface.
	txAdapter struct {
		dialect.Tx
	}
)

// New decorates the given driver.
func New(drv dialect.Driver) dialect.Driver {
	return &Driver{drv}
}

// Exec adds cockroachDB retry and calls the underlying transaction Exec method.
func (d *Tx) Exec(ctx context.Context, query string, args, v interface{}) error {
	c := txAdapter{d.Tx}
	return crdb.ExecuteInTx(ctx, &c, func() error {
		return d.Tx.Exec(ctx, query, args, v)
	})
}

// Exec extracts the args and v from the query and calls the Exec command.
func (c *txAdapter) Exec(ctx context.Context, query string, i ...interface{}) error {
	var args, v interface{}
	if len(i) > 0 {
		args = i[0]
	}
	if len(i) > 1 {
		v = i[1:]
	}
	return c.Tx.Exec(ctx, query, args, v)
}

// Commit implements Commit interface.
func (c *txAdapter) Commit(context.Context) error {
	return c.Tx.Commit()
}

// Rollback implements Rollback interface.
func (c *txAdapter) Rollback(context.Context) error {
	return c.Tx.Rollback()
}
