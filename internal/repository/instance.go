package repository

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"time"
)

var ErrInstanceBusy = errors.New("taskrunner instance already running for this fact database")

const instanceKey int64 = 0x54525f53494e474c

// InstanceLock owns the supported single-server profile. It is deliberately
// independent of the transaction/CAS protections for cron occurrences.
type InstanceLock struct {
	mu     sync.Mutex
	conn   *sql.Conn
	file   *os.File
	path   string
	closed bool
}

func (s *Store) AcquireInstance(ctx context.Context) (*InstanceLock, error) {
	lock := &InstanceLock{}
	if s.pg {
		conn, err := s.db.Conn(ctx)
		if err != nil {
			return nil, fmt.Errorf("instance lock connection: %w", err)
		}
		var acquired bool
		if err := conn.QueryRowContext(ctx, `SELECT pg_try_advisory_lock($1)`, instanceKey).Scan(&acquired); err != nil {
			conn.Close()
			return nil, fmt.Errorf("instance lock acquire: %w", err)
		}
		if !acquired {
			conn.Close()
			return nil, ErrInstanceBusy
		}
		lock.conn = conn
	} else {
		actual, err := filepath.EvalSymlinks(s.target)
		if err != nil {
			return nil, fmt.Errorf("instance database identity: %w", err)
		}
		actual, err = filepath.Abs(actual)
		if err != nil {
			return nil, err
		}
		lock.path = actual + ".instance.lock"
		file, err := lockLocalFile(lock.path)
		if err != nil {
			return nil, err
		}
		lock.file = file
	}
	return lock, nil
}

func (l *InstanceLock) Check(ctx context.Context) error {
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.closed {
		return errors.New("taskrunner instance ownership lost")
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	if l.conn != nil {
		var held bool
		err := l.conn.QueryRowContext(ctx, `SELECT EXISTS(SELECT 1 FROM pg_locks WHERE locktype='advisory' AND pid=pg_backend_pid() AND classid=$1::oid AND objid=$2::oid AND objsubid=1 AND granted)`, instanceKey>>32, instanceKey&0xffffffff).Scan(&held)
		if err != nil {
			return fmt.Errorf("instance ownership unverifiable: %w", err)
		}
		if !held {
			return errors.New("taskrunner instance ownership lost")
		}
		return nil
	}
	before, err := l.file.Stat()
	if err != nil {
		return fmt.Errorf("instance file ownership: %w", err)
	}
	current, err := os.Stat(l.path)
	if err != nil || !os.SameFile(before, current) {
		return errors.New("taskrunner instance lock file replaced or removed")
	}
	return nil
}
func (l *InstanceLock) Close() error {
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.closed {
		return nil
	}
	l.closed = true
	if l.conn != nil {
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()
		_, err := l.conn.ExecContext(ctx, `SELECT pg_advisory_unlock($1)`, instanceKey)
		// Never return a pooled physical session which could retain the advisory lock
		// after an uncertain unlock. Closing the session releases it server-side.
		_ = l.conn.Raw(func(any) error { return driver.ErrBadConn })
		closeErr := l.conn.Close()
		if err != nil {
			return err
		}
		return closeErr
	}
	return l.file.Close() // kernel releases flock even on process exit
}
