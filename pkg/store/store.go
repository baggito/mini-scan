package store

import (
	"context"
	"fmt"
	"time"
)

// ServiceRecord represents a stored scan result
type ServiceRecord struct {
	IP            string
	Port          uint32
	Service       string
	LastTimestamp int64
	Response      string
	UpdatedAt     time.Time
}

// Store defines the interface for scan data persistence
type Store interface {
	// Upsert inserts or updates a record if the timestamp is newer
	// Returns true if the record was inserted/updated, false if skipped (older timestamp)
	Upsert(ctx context.Context, record *ServiceRecord) (bool, error)

	// Get retrieves a record by its composite key
	// Returns nil, nil if not found
	Get(ctx context.Context, ip string, port uint32, service string) (*ServiceRecord, error)

	// List returns all records with optional pagination
	// Use limit=0 to return all records
	List(ctx context.Context, limit, offset int) ([]*ServiceRecord, error)

	// Close releases any resources held by the store
	Close() error
}

// NewStore creates a new store instance based on the store type
func NewStore(storeType, connectionString string) (Store, error) {
	switch storeType {
	case "sqlite":
		return NewSQLiteStore(connectionString)
	case "memory":
		return NewMemoryStore(), nil
	case "postgres":
		return NewPostgresStore(connectionString)
	default:
		return nil, fmt.Errorf("unknown store type: %s", storeType)
	}
}