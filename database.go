/*
Package sqln maintains a map of queries to named statements and uses named
statements for all operations.
*/
package sqln

import (
	"context"
	"database/sql"
	"sync"

	"github.com/jmoiron/sqlx"
	"github.com/pkg/errors"
)

// New wraps a sqlx database.
func New(dbx *sqlx.DB) *Database {
	return &Database{
		X:        dbx,
		stmtsMtx: &sync.Mutex{},
		stmts:    make(map[string]*sqlx.NamedStmt),
	}
}

// DB is an interface to allow for the Transact method to return a non-concrete type
// which is useful when wrapping this implementation.
type DB interface {
	Exec(ctx context.Context, query string, params interface{}) (sql.Result, error)
	Get(ctx context.Context, query string, dest, params interface{}) error
	Select(ctx context.Context, query string, dest, params interface{}) error

	// Stmt creates a named statement if one does not exist. It is not safe
	// to Close the returned statement.
	Stmt(query string) (*sqlx.NamedStmt, error)

	// TODO: Implement nested transactions.
	Transact(ctx context.Context, opts sql.TxOptions, f func(DB) error) error
}

// Database wraps a sqlx.DB and manages NamedStmt's.
type Database struct {
	X *sqlx.DB

	tx      *sqlx.Tx
	txLevel int

	// stmtsMtx serializes access to the stmts map.
	stmtsMtx *sync.Mutex
	stmts    map[string]*sqlx.NamedStmt
}

// Exec a SQL statement.
func (d *Database) Exec(ctx context.Context, query string, params interface{}) (sql.Result, error) {
	s, err := d.Stmt(query)
	if err != nil {
		return nil, err
	}

	if params == nil {
		params = struct{}{}
	}

	exec := s.ExecContext
	if d.tx != nil {
		exec = d.tx.NamedStmt(s).ExecContext
	}
	return exec(ctx, params)
}

// Get a single record.
func (d *Database) Get(ctx context.Context, query string, dest, params interface{}) error {
	s, err := d.Stmt(query)
	if err != nil {
		return err
	}

	if params == nil {
		params = struct{}{}
	}

	get := s.GetContext
	if d.tx != nil {
		get = d.tx.NamedStmt(s).GetContext
	}
	if err := get(ctx, dest, params); err != nil {
		return err
	}

	return nil
}

// Select multiple records.
func (d *Database) Select(ctx context.Context, query string, dest, params interface{}) error {
	s, err := d.Stmt(query)
	if err != nil {
		return err
	}

	if params == nil {
		params = struct{}{}
	}

	sel := s.SelectContext
	if d.tx != nil {
		sel = d.tx.NamedStmt(s).SelectContext
	}
	if err := sel(ctx, dest, params); err != nil {
		return err
	}

	return nil
}

// Transact will run the function that is passed in, rolling back all SQL
// statements if an error is returned.
// NOTE: A non-nil TxOptions struct is accepted to encourage thoughtful
// selection of transaction isolation levels.
// NOTE: Nested transactions are not currently supported and will return an error.
func (d *Database) Transact(ctx context.Context, opts sql.TxOptions, f func(DB) error) error {
	if d.tx != nil {
		// TODO: Support nested tx.
		return errors.New("nested tx not currently supported")
	}

	tx, err := d.X.BeginTxx(ctx, &opts)
	if err != nil {
		return err
	}

	txLvl := d.txLevel + 1
	if err := f(&Database{
		X:        d.X,
		tx:       tx,
		txLevel:  txLvl,
		stmtsMtx: d.stmtsMtx,
		stmts:    d.stmts,
	}); err != nil {
		if err := tx.Rollback(); err != nil {
			return errors.Wrapf(err, "tx level %v: rollback", txLvl)
		}
		return errors.Wrapf(err, "tx level %v", txLvl)
	}

	return errors.Wrapf(tx.Commit(), "tx level %v: commit", txLvl)
}

// Stmt creates and/or retrieves a named statement.
func (d *Database) Stmt(query string) (*sqlx.NamedStmt, error) {
	// Fetch an already-prepared statement.
	d.stmtsMtx.Lock()
	defer d.stmtsMtx.Unlock()

	s, ok := d.stmts[query]
	if ok {
		return s, nil
	}

	// Prepare the named statement.
	stmt, err := d.X.PrepareNamed(query)
	if err != nil {
		return nil, err
	}
	d.stmts[query] = stmt
	return stmt, nil
}

// Close all managed named statements. Does not close underlying *sqlx.DB.
func (d *Database) Close() error {
	d.stmtsMtx.Lock()
	defer d.stmtsMtx.Unlock()
	for _, stmt := range d.stmts {
		if err := stmt.Close(); err != nil {
			return err
		}
	}
	return nil
}
