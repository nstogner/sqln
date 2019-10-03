package sqln

import (
	"context"
	"database/sql"
	"log"
	"testing"

	"github.com/jmoiron/sqlx"
	_ "github.com/lib/pq"
	"github.com/nstogner/psqlxtest"
	"github.com/pkg/errors"
)

func ExampleUsage() {
	type SignupUserRequest struct {
		ID    string `db:"id"`
		Email string `db:"email"`
	}

	SignupUser := func(ctx context.Context, db DB, req SignupUserRequest) error {
		var n int
		if err := db.Select(ctx, "SELECT COUNT(*) FROM users WHERE id = :id;", &n, req); err != nil {
			return err
		}
		if n > 0 {
			return errors.New("user already exists")
		}

		if _, err := db.Exec(ctx, "INSERT INTO users (id, email) VALUES (:id, :email);", req); err != nil {
			return err
		}

		return nil
	}

	// Setup dependencies.
	dbx, err := sqlx.Connect("postgres", "postgres://postgres:postgres@localhost/test?sslmode=disable&timezone=utc")
	if err != nil {
		log.Fatal("unable to connect to db:", err)
	}
	defer dbx.Close()
	dbn := New(dbx)
	defer dbn.Close()

	ctx := context.Background()

	// Run SingupUser outside of a transaction.
	_ = SignupUser(ctx, dbn, SignupUserRequest{
		ID:    "nick1234",
		Email: "nick000@gmail.com",
	})

	// Run SignupUser inside a transaction with other stuff.
	_ = dbn.Transact(ctx, sql.TxOptions{Isolation: sql.LevelSerializable}, func(db DB) error {
		if err := SignupUser(ctx, db, SignupUserRequest{
			ID:    "nick1234",
			Email: "nick000@gmail.com",
		}); err != nil {
			return err
		}

		// Something else here...

		return nil
	})
}

func TestDB(t *testing.T) {
	var d *Database
	{
		dbx, dropx := psqlxtest.TmpDB(t)
		defer dropx()

		d = New(dbx)
		defer func() {
			if err := d.Close(); err != nil {
				t.Fatalf("closing sqln database: %v", err)
			}
		}()
	}

	ctx := context.Background()

	if _, err := d.X.Exec("DROP TABLE IF EXISTS abc;"); err != nil {
		t.Fatal("unable to create table:", err)
	}
	if _, err := d.X.Exec("CREATE TABLE abc (id INT, x INT, PRIMARY KEY(id));"); err != nil {
		t.Fatal("unable to create table:", err)
	}

	const insert = "INSERT INTO abc (id,x) VALUES (:id,:x);"
	if _, err := d.Stmt(insert); err != nil {
		t.Fatal(err)
	}

	err := d.Transact(ctx, sql.TxOptions{Isolation: sql.LevelSerializable}, func(tx DB) error {
		if tx.(*Database).tx == nil {
			t.Fatal("tx should not be nil")
		}
		if _, err := tx.Exec(ctx, insert, map[string]interface{}{"id": 1, "x": 1}); err != nil {
			return errors.Wrap(err, "unable to insert (1,1)")
		}
		if _, err := tx.Exec(ctx, insert, map[string]interface{}{"id": 2, "x": 2}); err != nil {
			return errors.Wrap(err, "unable to insert (2,2)")
		}
		if _, err := tx.Exec(ctx, insert, map[string]interface{}{"id": 1, "x": 1}); err != nil {
			return errors.Wrap(err, "unable to insert (1,1)")
		}
		return nil
	})
	if err == nil {
		t.Fatal("expected transaction to fail")
	}

	if d.tx != nil {
		t.Fatal("tx should be nil")
	}

	var n int
	if err := d.Get(ctx, "SELECT COUNT(*) FROM abc;", &n, nil); err != nil {
		t.Fatal("unexpected error counting:", err)
	}
	if n != 0 {
		t.Fatal("expected n == 0")
	}

	if err := d.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}
}
