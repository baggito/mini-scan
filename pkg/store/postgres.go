package store

import (
	"context"
	"database/sql"
	"fmt"

	_ "github.com/lib/pq"
)

// PostgresStore implements Store interface using PostgreSQL
type PostgresStore struct {
	db *sql.DB
}

// NewPostgresStore creates a new PostgreSQL store
func NewPostgresStore(connStr string) (*PostgresStore, error) {
	db, err := sql.Open("postgres", connStr)
	if err != nil {
		return nil, fmt.Errorf("failed to open database: %w", err)
	}

	// Test connection
	if err := db.Ping(); err != nil {
		db.Close()
		return nil, fmt.Errorf("failed to ping database: %w", err)
	}

	// Create table if not exists
	_, err = db.Exec(`
		CREATE TABLE IF NOT EXISTS service_records (
			ip            TEXT NOT NULL,
			port          INTEGER NOT NULL,
			service       TEXT NOT NULL,
			last_timestamp BIGINT NOT NULL,
			response      TEXT NOT NULL,
			updated_at    TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
			PRIMARY KEY (ip, port, service)
		)
	`)
	if err != nil {
		db.Close()
		return nil, fmt.Errorf("failed to create table: %w", err)
	}

	// Create index for timestamp queries
	_, err = db.Exec(`CREATE INDEX IF NOT EXISTS idx_timestamp ON service_records(last_timestamp)`)
	if err != nil {
		db.Close()
		return nil, fmt.Errorf("failed to create index: %w", err)
	}

	return &PostgresStore{db: db}, nil
}

// Upsert inserts or updates a record if the timestamp is newer
func (s *PostgresStore) Upsert(ctx context.Context, r *ServiceRecord) (bool, error) {
	result, err := s.db.ExecContext(ctx, `
		INSERT INTO service_records (ip, port, service, last_timestamp, response, updated_at)
		VALUES ($1, $2, $3, $4, $5, CURRENT_TIMESTAMP)
		ON CONFLICT (ip, port, service) DO UPDATE SET
			last_timestamp = EXCLUDED.last_timestamp,
			response = EXCLUDED.response,
			updated_at = CURRENT_TIMESTAMP
		WHERE EXCLUDED.last_timestamp > service_records.last_timestamp
	`, r.IP, r.Port, r.Service, r.LastTimestamp, r.Response)

	if err != nil {
		return false, fmt.Errorf("failed to upsert record: %w", err)
	}

	rows, err := result.RowsAffected()
	if err != nil {
		return false, fmt.Errorf("failed to get rows affected: %w", err)
	}

	return rows > 0, nil
}

// Get retrieves a record by its composite key
func (s *PostgresStore) Get(ctx context.Context, ip string, port uint32, service string) (*ServiceRecord, error) {
	row := s.db.QueryRowContext(ctx, `
		SELECT ip, port, service, last_timestamp, response, updated_at
		FROM service_records
		WHERE ip = $1 AND port = $2 AND service = $3
	`, ip, port, service)

	var r ServiceRecord
	err := row.Scan(&r.IP, &r.Port, &r.Service, &r.LastTimestamp, &r.Response, &r.UpdatedAt)
	if err == sql.ErrNoRows {
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("failed to get record: %w", err)
	}

	return &r, nil
}

// List returns all records with optional pagination
func (s *PostgresStore) List(ctx context.Context, limit, offset int) ([]*ServiceRecord, error) {
	var rows *sql.Rows
	var err error

	if limit > 0 {
		rows, err = s.db.QueryContext(ctx, `
			SELECT ip, port, service, last_timestamp, response, updated_at
			FROM service_records
			ORDER BY last_timestamp DESC
			LIMIT $1 OFFSET $2
		`, limit, offset)
	} else {
		rows, err = s.db.QueryContext(ctx, `
			SELECT ip, port, service, last_timestamp, response, updated_at
			FROM service_records
			ORDER BY last_timestamp DESC
		`)
	}

	if err != nil {
		return nil, fmt.Errorf("failed to list records: %w", err)
	}
	defer rows.Close()

	var records []*ServiceRecord
	for rows.Next() {
		var r ServiceRecord
		if err := rows.Scan(&r.IP, &r.Port, &r.Service, &r.LastTimestamp, &r.Response, &r.UpdatedAt); err != nil {
			return nil, fmt.Errorf("failed to scan record: %w", err)
		}
		records = append(records, &r)
	}

	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("error iterating records: %w", err)
	}

	return records, nil
}

// Close closes the database connection
func (s *PostgresStore) Close() error {
	return s.db.Close()
}
