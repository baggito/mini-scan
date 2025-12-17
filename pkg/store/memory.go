package store

import (
	"context"
	"fmt"
	"sort"
	"sync"
	"time"
)

// MemoryStore implements Store interface using in-memory storage
// Useful for testing
type MemoryStore struct {
	mu      sync.RWMutex
	records map[string]*ServiceRecord // key: "ip:port:service"
}

// NewMemoryStore creates a new in-memory store
func NewMemoryStore() *MemoryStore {
	return &MemoryStore{
		records: make(map[string]*ServiceRecord),
	}
}

// makeKey creates a composite key from ip, port, and service
func makeKey(ip string, port uint32, service string) string {
	return fmt.Sprintf("%s:%d:%s", ip, port, service)
}

// Upsert inserts or updates a record if the timestamp is newer
func (s *MemoryStore) Upsert(ctx context.Context, r *ServiceRecord) (bool, error) {
	// Acquire exclusive lock for writing - blocks other reads and writes until unlocked
	s.mu.Lock()
	defer s.mu.Unlock()

	key := makeKey(r.IP, r.Port, r.Service)
	existing, exists := s.records[key]

	if !exists || r.LastTimestamp > existing.LastTimestamp {
		// Create a copy to avoid external mutation
		record := &ServiceRecord{
			IP:            r.IP,
			Port:          r.Port,
			Service:       r.Service,
			LastTimestamp: r.LastTimestamp,
			Response:      r.Response,
			UpdatedAt:     time.Now(),
		}
		s.records[key] = record
		return true, nil
	}

	// Older record, skip
	return false, nil
}

// Get retrieves a record by its composite key
func (s *MemoryStore) Get(ctx context.Context, ip string, port uint32, service string) (*ServiceRecord, error) {
	// Acquire read lock - allows multiple concurrent readers, but blocks writers
	s.mu.RLock()
	defer s.mu.RUnlock()

	key := makeKey(ip, port, service)
	record, exists := s.records[key]
	if !exists {
		return nil, nil
	}

	// Return a copy to avoid external mutation
	return &ServiceRecord{
		IP:            record.IP,
		Port:          record.Port,
		Service:       record.Service,
		LastTimestamp: record.LastTimestamp,
		Response:      record.Response,
		UpdatedAt:     record.UpdatedAt,
	}, nil
}

// List returns all records with optional pagination
func (s *MemoryStore) List(ctx context.Context, limit, offset int) ([]*ServiceRecord, error) {
	// Acquire read lock - allows multiple concurrent readers, but blocks writers
	s.mu.RLock()
	defer s.mu.RUnlock()

	// Collect all records
	all := make([]*ServiceRecord, 0, len(s.records))
	for _, r := range s.records {
		all = append(all, &ServiceRecord{
			IP:            r.IP,
			Port:          r.Port,
			Service:       r.Service,
			LastTimestamp: r.LastTimestamp,
			Response:      r.Response,
			UpdatedAt:     r.UpdatedAt,
		})
	}

	// Sort by timestamp descending
	sort.Slice(all, func(i, j int) bool {
		return all[i].LastTimestamp > all[j].LastTimestamp
	})

	// Apply pagination
	if offset >= len(all) {
		return []*ServiceRecord{}, nil
	}

	all = all[offset:]

	if limit > 0 && limit < len(all) {
		all = all[:limit]
	}

	return all, nil
}

// Close is a no-op for memory store
func (s *MemoryStore) Close() error {
	return nil
}

// Len returns the number of records (useful for testing)
func (s *MemoryStore) Len() int {
	// Acquire read lock - allows multiple concurrent readers, but blocks writers
	s.mu.RLock()
	defer s.mu.RUnlock()
	return len(s.records)
}
