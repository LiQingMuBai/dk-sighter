package repository

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"hash/fnv"
	"strings"
	"sync"
)

var ErrNamedLockBusy = errors.New("named lock is already held")

type NamedLock struct {
	conn *sql.Conn
	name string

	mu       sync.Mutex
	released bool
}

func BuildProcessLockName(prefix, key string) string {
	prefix = strings.TrimSpace(prefix)
	if prefix == "" {
		prefix = "tron:"
	}

	key = strings.TrimSpace(key)
	if key == "" {
		key = "default"
	}

	lockName := prefix + key
	if len(lockName) <= 64 {
		return lockName
	}

	sum := fnv.New32a()
	_, _ = sum.Write([]byte(lockName))

	maxKeyLen := 64 - len(prefix) - 9
	if maxKeyLen < 1 {
		maxKeyLen = 1
	}
	if len(key) > maxKeyLen {
		key = key[:maxKeyLen]
	}

	return fmt.Sprintf("%s%s:%08x", prefix, key, sum.Sum32())
}

func (d *DB) AcquireNamedLock(ctx context.Context, name string) (*NamedLock, error) {
	if d == nil || d.sql == nil {
		return nil, fmt.Errorf("acquire named lock: database is nil")
	}

	name = strings.TrimSpace(name)
	if name == "" {
		return nil, fmt.Errorf("acquire named lock: empty lock name")
	}

	conn, err := d.sql.Conn(ctx)
	if err != nil {
		return nil, fmt.Errorf("acquire named lock %s: %w", name, err)
	}

	var acquired sql.NullInt64
	err = conn.QueryRowContext(ctx, `SELECT GET_LOCK(?, 0)`, name).Scan(&acquired)
	if err != nil {
		_ = conn.Close()
		return nil, fmt.Errorf("acquire named lock %s: %w", name, err)
	}
	if !acquired.Valid || acquired.Int64 != 1 {
		_ = conn.Close()
		return nil, ErrNamedLockBusy
	}

	return &NamedLock{
		conn: conn,
		name: name,
	}, nil
}

func (l *NamedLock) Release(ctx context.Context) error {
	if l == nil {
		return nil
	}

	l.mu.Lock()
	if l.released {
		l.mu.Unlock()
		return nil
	}
	l.released = true
	conn := l.conn
	name := l.name
	l.mu.Unlock()

	if conn == nil {
		return nil
	}

	var released sql.NullInt64
	err := conn.QueryRowContext(ctx, `SELECT RELEASE_LOCK(?)`, name).Scan(&released)
	closeErr := conn.Close()
	if err != nil {
		return fmt.Errorf("release named lock %s: %w", name, err)
	}
	if closeErr != nil {
		return fmt.Errorf("close named lock %s connection: %w", name, closeErr)
	}
	return nil
}
