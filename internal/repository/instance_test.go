package repository

import (
	"context"
	"database/sql"
	"errors"
	"os"
	"path/filepath"
	"testing"
	"time"
)

func TestSQLiteInstanceSameFileAndSymlink(t *testing.T) {
	path := filepath.Join(t.TempDir(), "facts.db")
	st, err := Open(path)
	if err != nil {
		t.Fatal(err)
	}
	defer st.Close()
	first, err := st.AcquireInstance(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	defer first.Close()
	alias := path + ".alias"
	if err := os.Symlink(path, alias); err != nil {
		t.Fatal(err)
	}
	other, err := Open(alias)
	if err != nil {
		t.Fatal(err)
	}
	defer other.Close()
	if lease, err := other.AcquireInstance(context.Background()); !errors.Is(err, ErrInstanceBusy) {
		if lease != nil {
			lease.Close()
		}
		t.Fatalf("same fact file must be exclusive: %v", err)
	}
	if err := first.Check(context.Background()); err != nil {
		t.Fatal(err)
	}
	if err := first.Close(); err != nil {
		t.Fatal(err)
	}
	replacement, err := other.AcquireInstance(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	defer replacement.Close()
	if err := first.Check(context.Background()); err == nil {
		t.Fatal("closed ownership is not valid")
	}
	if err := os.Remove(replacement.path); err != nil {
		t.Fatal(err)
	}
	if err := replacement.Check(context.Background()); err == nil {
		t.Fatal("removed/replaced lock inode must fail closed")
	}
}
func TestPGInstanceSessionOwnership(t *testing.T) {
	st := openTestStorePG(t)
	ctx := context.Background()
	first, err := st.AcquireInstance(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer first.Close()
	if other, err := st.AcquireInstance(ctx); !errors.Is(err, ErrInstanceBusy) {
		if other != nil {
			other.Close()
		}
		t.Fatalf("second PG instance accepted: %v", err)
	}
	if err := first.Check(ctx); err != nil {
		t.Fatal(err)
	}
	var pid int
	if err := first.conn.QueryRowContext(ctx, `SELECT pg_backend_pid()`).Scan(&pid); err != nil {
		t.Fatal(err)
	}
	admin, err := sql.Open("pgx", os.Getenv("TASKRUNNER_TEST_PG_DSN"))
	if err != nil {
		t.Fatal(err)
	}
	defer admin.Close()
	if _, err := admin.ExecContext(ctx, `SELECT pg_terminate_backend($1)`, pid); err != nil {
		t.Fatal(err)
	}
	checkCtx, cancel := context.WithTimeout(ctx, time.Second)
	defer cancel()
	if err := first.Check(checkCtx); err == nil {
		t.Fatal("killed advisory-lock session remained valid")
	}
	replacement, err := st.AcquireInstance(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer replacement.Close()
	if err := replacement.Check(ctx); err != nil {
		t.Fatal(err)
	}
}
