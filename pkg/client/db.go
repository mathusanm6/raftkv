package client

import (
	"context"
	"database/sql"
	"errors"
	"sync"

	_ "github.com/lib/pq"
)

var (
	ErrNotFound = errors.New("key not found in database")
)

// DB defines the source-of-truth database interface matching design doc §2.3.
type DB interface {
	Get(ctx context.Context, key string) (value string, version int64, err error)
	Update(ctx context.Context, key, value string) (newVersion int64, err error)
	Close() error
}

// PostgresDB implements DB backed by a real PostgreSQL instance.
type PostgresDB struct {
	db *sql.DB
}

// NewPostgresDB connects to a PostgreSQL instance.
func NewPostgresDB(connStr string, maxConns int) (*PostgresDB, error) {
	db, err := sql.Open("postgres", connStr)
	if err != nil {
		return nil, err
	}
	if maxConns > 0 {
		db.SetMaxOpenConns(maxConns)
		db.SetMaxIdleConns(maxConns / 2)
	}
	return &PostgresDB{db: db}, nil
}

// Get selects value and version from PostgreSQL.
func (p *PostgresDB) Get(ctx context.Context, key string) (string, int64, error) {
	var value string
	var version int64
	err := p.db.QueryRowContext(ctx, "SELECT value, version FROM items WHERE key = $1", key).Scan(&value, &version)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return "", 0, ErrNotFound
		}
		return "", 0, err
	}
	return value, version, nil
}

// Update increments version and returns the new version.
func (p *PostgresDB) Update(ctx context.Context, key, value string) (int64, error) {
	var newVersion int64
	err := p.db.QueryRowContext(ctx, "UPDATE items SET value = $2, version = version + 1 WHERE key = $1 RETURNING version", key, value).Scan(&newVersion)
	if err != nil {
		return 0, err
	}
	return newVersion, nil
}

// Close closes the database connection pool.
func (p *PostgresDB) Close() error {
	return p.db.Close()
}

// MockDB provides an in-memory thread-safe DB implementation for testing and development.
type MockDB struct {
	mu   sync.RWMutex
	data map[string]mockEntry
}

type mockEntry struct {
	value   string
	version int64
}

// NewMockDB creates a new in-memory MockDB.
func NewMockDB() *MockDB {
	return &MockDB{
		data: make(map[string]mockEntry),
	}
}

// SetInitial seeds an initial entry with version 1.
func (m *MockDB) SetInitial(key, value string, version int64) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.data[key] = mockEntry{value: value, version: version}
}

// Get reads from in-memory mock DB.
func (m *MockDB) Get(_ context.Context, key string) (string, int64, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	entry, ok := m.data[key]
	if !ok {
		return "", 0, ErrNotFound
	}
	return entry.value, entry.version, nil
}

// Update updates value and increments version.
func (m *MockDB) Update(_ context.Context, key, value string) (int64, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	entry, ok := m.data[key]
	newVer := int64(1)
	if ok {
		newVer = entry.version + 1
	}
	m.data[key] = mockEntry{value: value, version: newVer}
	return newVer, nil
}

// Close is a no-op for MockDB.
func (m *MockDB) Close() error {
	return nil
}
