package store

import (
	"context"
	"os"
	"testing"
	"time"
)

// TestMemoryStore tests the in-memory store implementation
func TestMemoryStore(t *testing.T) {
	store := NewMemoryStore()
	defer store.Close()

	runStoreTests(t, store)
}

// TestSQLiteStore tests the SQLite store implementation
func TestSQLiteStore(t *testing.T) {
	// Create a temporary database file
	tmpFile, err := os.CreateTemp("", "test-*.db")
	if err != nil {
		t.Fatalf("Failed to create temp file: %v", err)
	}
	tmpFile.Close()
	defer os.Remove(tmpFile.Name())

	store, err := NewSQLiteStore(tmpFile.Name())
	if err != nil {
		t.Fatalf("Failed to create SQLite store: %v", err)
	}
	defer store.Close()

	runStoreTests(t, store)
}

// runStoreTests runs common tests for any Store implementation
func runStoreTests(t *testing.T, s Store) {
	ctx := context.Background()

	t.Run("Upsert new record", func(t *testing.T) {
		record := &ServiceRecord{
			IP:            "1.1.1.1",
			Port:          80,
			Service:       "HTTP",
			LastTimestamp: 1000,
			Response:      "test response",
		}

		updated, err := s.Upsert(ctx, record)
		if err != nil {
			t.Fatalf("Upsert failed: %v", err)
		}
		if !updated {
			t.Error("Expected record to be inserted")
		}

		// Verify the record was stored
		got, err := s.Get(ctx, "1.1.1.1", 80, "HTTP")
		if err != nil {
			t.Fatalf("Get failed: %v", err)
		}
		if got == nil {
			t.Fatal("Expected record to exist")
		}
		if got.Response != "test response" {
			t.Errorf("Expected response 'test response', got '%s'", got.Response)
		}
		if got.LastTimestamp != 1000 {
			t.Errorf("Expected timestamp 1000, got %d", got.LastTimestamp)
		}
	})

	t.Run("Upsert with newer timestamp", func(t *testing.T) {
		record := &ServiceRecord{
			IP:            "1.1.1.1",
			Port:          80,
			Service:       "HTTP",
			LastTimestamp: 2000, // Newer
			Response:      "newer response",
		}

		updated, err := s.Upsert(ctx, record)
		if err != nil {
			t.Fatalf("Upsert failed: %v", err)
		}
		if !updated {
			t.Error("Expected record to be updated")
		}

		// Verify the record was updated
		got, err := s.Get(ctx, "1.1.1.1", 80, "HTTP")
		if err != nil {
			t.Fatalf("Get failed: %v", err)
		}
		if got.Response != "newer response" {
			t.Errorf("Expected response 'newer response', got '%s'", got.Response)
		}
		if got.LastTimestamp != 2000 {
			t.Errorf("Expected timestamp 2000, got %d", got.LastTimestamp)
		}
	})

	t.Run("Upsert with older timestamp (out-of-order)", func(t *testing.T) {
		record := &ServiceRecord{
			IP:            "1.1.1.1",
			Port:          80,
			Service:       "HTTP",
			LastTimestamp: 500, // Older than 2000
			Response:      "old response",
		}

		updated, err := s.Upsert(ctx, record)
		if err != nil {
			t.Fatalf("Upsert failed: %v", err)
		}
		if updated {
			t.Error("Expected record NOT to be updated (older timestamp)")
		}

		// Verify the record was NOT updated
		got, err := s.Get(ctx, "1.1.1.1", 80, "HTTP")
		if err != nil {
			t.Fatalf("Get failed: %v", err)
		}
		if got.Response != "newer response" {
			t.Errorf("Expected response to remain 'newer response', got '%s'", got.Response)
		}
		if got.LastTimestamp != 2000 {
			t.Errorf("Expected timestamp to remain 2000, got %d", got.LastTimestamp)
		}
	})

	t.Run("Upsert with equal timestamp", func(t *testing.T) {
		record := &ServiceRecord{
			IP:            "1.1.1.1",
			Port:          80,
			Service:       "HTTP",
			LastTimestamp: 2000, // Same as current
			Response:      "duplicate response",
		}

		updated, err := s.Upsert(ctx, record)
		if err != nil {
			t.Fatalf("Upsert failed: %v", err)
		}
		if updated {
			t.Error("Expected record NOT to be updated (equal timestamp)")
		}

		// Verify the record was NOT updated
		got, err := s.Get(ctx, "1.1.1.1", 80, "HTTP")
		if err != nil {
			t.Fatalf("Get failed: %v", err)
		}
		if got.Response != "newer response" {
			t.Errorf("Expected response to remain 'newer response', got '%s'", got.Response)
		}
	})

	t.Run("Get non-existent record", func(t *testing.T) {
		got, err := s.Get(ctx, "9.9.9.9", 9999, "UNKNOWN")
		if err != nil {
			t.Fatalf("Get failed: %v", err)
		}
		if got != nil {
			t.Error("Expected nil for non-existent record")
		}
	})

	t.Run("Different composite keys", func(t *testing.T) {
		// Add records with different keys
		records := []*ServiceRecord{
			{IP: "2.2.2.2", Port: 443, Service: "HTTPS", LastTimestamp: 1000, Response: "https response"},
			{IP: "2.2.2.2", Port: 22, Service: "SSH", LastTimestamp: 1000, Response: "ssh response"},
			{IP: "3.3.3.3", Port: 443, Service: "HTTPS", LastTimestamp: 1000, Response: "another https"},
		}

		for _, r := range records {
			_, err := s.Upsert(ctx, r)
			if err != nil {
				t.Fatalf("Upsert failed: %v", err)
			}
		}

		// Verify each record exists independently
		got1, _ := s.Get(ctx, "2.2.2.2", 443, "HTTPS")
		got2, _ := s.Get(ctx, "2.2.2.2", 22, "SSH")
		got3, _ := s.Get(ctx, "3.3.3.3", 443, "HTTPS")

		if got1 == nil || got1.Response != "https response" {
			t.Error("Record 1 not found or incorrect")
		}
		if got2 == nil || got2.Response != "ssh response" {
			t.Error("Record 2 not found or incorrect")
		}
		if got3 == nil || got3.Response != "another https" {
			t.Error("Record 3 not found or incorrect")
		}
	})

	t.Run("List records", func(t *testing.T) {
		records, err := s.List(ctx, 0, 0)
		if err != nil {
			t.Fatalf("List failed: %v", err)
		}
		if len(records) < 4 {
			t.Errorf("Expected at least 4 records, got %d", len(records))
		}
	})

	t.Run("List with pagination", func(t *testing.T) {
		records, err := s.List(ctx, 2, 0)
		if err != nil {
			t.Fatalf("List failed: %v", err)
		}
		if len(records) != 2 {
			t.Errorf("Expected 2 records with limit, got %d", len(records))
		}

		// Test offset
		records2, err := s.List(ctx, 2, 2)
		if err != nil {
			t.Fatalf("List with offset failed: %v", err)
		}
		if len(records2) < 1 {
			t.Errorf("Expected at least 1 record with offset, got %d", len(records2))
		}
	})
}

// TestMemoryStoreLen tests the Len helper method on MemoryStore
func TestMemoryStoreLen(t *testing.T) {
	store := NewMemoryStore()
	defer store.Close()

	ctx := context.Background()

	if store.Len() != 0 {
		t.Errorf("Expected empty store, got %d records", store.Len())
	}

	store.Upsert(ctx, &ServiceRecord{
		IP: "1.1.1.1", Port: 80, Service: "HTTP",
		LastTimestamp: 1000, Response: "test",
	})

	if store.Len() != 1 {
		t.Errorf("Expected 1 record, got %d", store.Len())
	}
}

// TestUpdatedAt tests that UpdatedAt is set correctly
func TestUpdatedAt(t *testing.T) {
	store := NewMemoryStore()
	defer store.Close()

	ctx := context.Background()
	before := time.Now()

	store.Upsert(ctx, &ServiceRecord{
		IP: "1.1.1.1", Port: 80, Service: "HTTP",
		LastTimestamp: 1000, Response: "test",
	})

	after := time.Now()

	got, _ := store.Get(ctx, "1.1.1.1", 80, "HTTP")
	if got.UpdatedAt.Before(before) || got.UpdatedAt.After(after) {
		t.Errorf("UpdatedAt not in expected range")
	}
}
