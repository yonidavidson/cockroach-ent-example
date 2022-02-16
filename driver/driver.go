package driver

import (
	"context"
	"database/sql"
	"fmt"

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
	// txAdapter implements crdb TX interface.
	txAdapter struct {
		dialect.Tx
	}
)

// New decorates the given driver.
func New(drv dialect.Driver) dialect.Driver {
	return &Driver{drv}
}

// Exec calls the underlying driver Exec method.
func (d *Driver) Exec(ctx context.Context, query string, args, v interface{}) error {
	return d.Driver.Exec(ctx, query, args, v)
}

// Query calls the underlying driver Query method.
func (d *Driver) Query(ctx context.Context, query string, args, v interface{}) error {
	return d.Driver.Query(ctx, query, args, v)
}

// Tx implements the underlying Tx command.
func (d *Driver) Tx(ctx context.Context) (dialect.Tx, error) {
	tx, err := d.Driver.Tx(ctx)
	if err != nil {
		return nil, err
	}
	return &Tx{tx, ctx}, nil
}

// BeginTx implements the underlying driver BeginTx command if it's supported.
func (d *Driver) BeginTx(ctx context.Context, opts *sql.TxOptions) (dialect.Tx, error) {
	drv, ok := d.Driver.(interface {
		BeginTx(context.Context, *sql.TxOptions) (dialect.Tx, error)
	})
	if !ok {
		return nil, fmt.Errorf("Driver.BeginTx is not supported")
	}
	tx, err := drv.BeginTx(ctx, opts)
	if err != nil {
		return nil, err
	}
	return &Tx{tx, ctx}, nil
}

// Exec adds cockroachDB retry and calls the underlying transaction Exec method.
func (d *Tx) Exec(ctx context.Context, query string, args, v interface{}) error {
	c := txAdapter{d.Tx}
	return crdb.ExecuteInTx(ctx, &c, func() error {
		return d.Tx.Exec(ctx, query, args, v)
	})
}

// Query calls the underlying transaction Query method.
func (d *Tx) Query(ctx context.Context, query string, args, v interface{}) error {
	return d.Tx.Query(ctx, query, args, v)
}

// Commit commits the underlying Tx.
func (d *Tx) Commit() error {
	return d.Tx.Commit()
}

// Rollback rolls back the underlying Tx.
func (d *Tx) Rollback() error {
	return d.Tx.Rollback()
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
