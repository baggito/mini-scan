package processor

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"testing"

	"github.com/censys/scan-takehome/pkg/scanning"
	"github.com/censys/scan-takehome/pkg/store"
)

// TestProcessV1Message tests processing of V1 format messages (base64 encoded)
func TestProcessV1Message(t *testing.T) {
	memStore := store.NewMemoryStore()
	defer memStore.Close()

	proc := NewProcessor(memStore)
	ctx := context.Background()

	// Create a V1 message with base64 encoded response
	responseStr := "hello world"
	responseBase64 := base64.StdEncoding.EncodeToString([]byte(responseStr))

	v1Data := map[string]string{
		"response_bytes_utf8": responseBase64,
	}
	v1DataJSON, _ := json.Marshal(v1Data)

	message := map[string]interface{}{
		"ip":           "1.1.1.1",
		"port":         uint32(80),
		"service":      "HTTP",
		"timestamp":    int64(1000),
		"data_version": scanning.V1,
		"data":         json.RawMessage(v1DataJSON),
	}
	messageJSON, _ := json.Marshal(message)

	// Process the message
	err := proc.Process(ctx, messageJSON)
	if err != nil {
		t.Fatalf("Process failed: %v", err)
	}

	// Verify the record was stored correctly
	record, err := memStore.Get(ctx, "1.1.1.1", 80, "HTTP")
	if err != nil {
		t.Fatalf("Get failed: %v", err)
	}
	if record == nil {
		t.Fatal("Expected record to exist")
	}
	if record.Response != responseStr {
		t.Errorf("Expected response '%s', got '%s'", responseStr, record.Response)
	}
	if record.LastTimestamp != 1000 {
		t.Errorf("Expected timestamp 1000, got %d", record.LastTimestamp)
	}
}

// TestProcessV2Message tests processing of V2 format messages (plain string)
func TestProcessV2Message(t *testing.T) {
	memStore := store.NewMemoryStore()
	defer memStore.Close()

	proc := NewProcessor(memStore)
	ctx := context.Background()

	// Create a V2 message with plain string response
	responseStr := "hello world v2"

	v2Data := map[string]string{
		"response_str": responseStr,
	}
	v2DataJSON, _ := json.Marshal(v2Data)

	message := map[string]interface{}{
		"ip":           "2.2.2.2",
		"port":         uint32(443),
		"service":      "HTTPS",
		"timestamp":    int64(2000),
		"data_version": scanning.V2,
		"data":         json.RawMessage(v2DataJSON),
	}
	messageJSON, _ := json.Marshal(message)

	// Process the message
	err := proc.Process(ctx, messageJSON)
	if err != nil {
		t.Fatalf("Process failed: %v", err)
	}

	// Verify the record was stored correctly
	record, err := memStore.Get(ctx, "2.2.2.2", 443, "HTTPS")
	if err != nil {
		t.Fatalf("Get failed: %v", err)
	}
	if record == nil {
		t.Fatal("Expected record to exist")
	}
	if record.Response != responseStr {
		t.Errorf("Expected response '%s', got '%s'", responseStr, record.Response)
	}
}

// TestProcessOutOfOrder tests that out-of-order messages are handled correctly
func TestProcessOutOfOrder(t *testing.T) {
	memStore := store.NewMemoryStore()
	defer memStore.Close()

	proc := NewProcessor(memStore)
	ctx := context.Background()

	// Helper to create a V2 message
	createMessage := func(timestamp int64, response string) []byte {
		v2Data := map[string]string{"response_str": response}
		v2DataJSON, _ := json.Marshal(v2Data)

		message := map[string]interface{}{
			"ip":           "3.3.3.3",
			"port":         uint32(22),
			"service":      "SSH",
			"timestamp":    timestamp,
			"data_version": scanning.V2,
			"data":         json.RawMessage(v2DataJSON),
		}
		messageJSON, _ := json.Marshal(message)
		return messageJSON
	}

	// Process messages in this order: 1000, 2000, 500, 1500, 3000
	tests := []struct {
		timestamp      int64
		response       string
		expectUpdate   bool
		expectedFinal  string
	}{
		{1000, "response 1000", true, "response 1000"},
		{2000, "response 2000", true, "response 2000"},
		{500, "response 500", false, "response 2000"},   // Out of order, should be skipped
		{1500, "response 1500", false, "response 2000"}, // Out of order, should be skipped
		{3000, "response 3000", true, "response 3000"},
	}

	for _, tt := range tests {
		err := proc.Process(ctx, createMessage(tt.timestamp, tt.response))
		if err != nil {
			t.Fatalf("Process failed for timestamp %d: %v", tt.timestamp, err)
		}

		record, _ := memStore.Get(ctx, "3.3.3.3", 22, "SSH")
		if record.Response != tt.expectedFinal {
			t.Errorf("After timestamp %d: expected response '%s', got '%s'",
				tt.timestamp, tt.expectedFinal, record.Response)
		}
	}
}

// TestProcessInvalidJSON tests handling of invalid JSON
func TestProcessInvalidJSON(t *testing.T) {
	memStore := store.NewMemoryStore()
	defer memStore.Close()

	proc := NewProcessor(memStore)
	ctx := context.Background()

	err := proc.Process(ctx, []byte("not valid json"))
	if err == nil {
		t.Error("Expected error for invalid JSON")
	}
}

// TestProcessUnknownVersion tests handling of unknown data version
func TestProcessUnknownVersion(t *testing.T) {
	memStore := store.NewMemoryStore()
	defer memStore.Close()

	proc := NewProcessor(memStore)
	ctx := context.Background()

	message := map[string]interface{}{
		"ip":           "4.4.4.4",
		"port":         uint32(80),
		"service":      "HTTP",
		"timestamp":    int64(1000),
		"data_version": 999, // Unknown version
		"data":         json.RawMessage(`{}`),
	}
	messageJSON, _ := json.Marshal(message)

	err := proc.Process(ctx, messageJSON)
	if err == nil {
		t.Error("Expected error for unknown data version")
	}
}

// TestProcessMultipleServices tests processing multiple different services
func TestProcessMultipleServices(t *testing.T) {
	memStore := store.NewMemoryStore()
	defer memStore.Close()

	proc := NewProcessor(memStore)
	ctx := context.Background()

	// Helper to create a V2 message
	createMessage := func(ip string, port uint32, service string, timestamp int64, response string) []byte {
		v2Data := map[string]string{"response_str": response}
		v2DataJSON, _ := json.Marshal(v2Data)

		message := map[string]interface{}{
			"ip":           ip,
			"port":         port,
			"service":      service,
			"timestamp":    timestamp,
			"data_version": scanning.V2,
			"data":         json.RawMessage(v2DataJSON),
		}
		messageJSON, _ := json.Marshal(message)
		return messageJSON
	}

	// Process different services
	messages := []struct {
		ip       string
		port     uint32
		service  string
		response string
	}{
		{"1.1.1.1", 80, "HTTP", "http response"},
		{"1.1.1.1", 443, "HTTPS", "https response"},
		{"1.1.1.1", 22, "SSH", "ssh response"},
		{"1.1.1.2", 80, "HTTP", "another http"},
	}

	for _, m := range messages {
		err := proc.Process(ctx, createMessage(m.ip, m.port, m.service, 1000, m.response))
		if err != nil {
			t.Fatalf("Process failed for %s:%d/%s: %v", m.ip, m.port, m.service, err)
		}
	}

	// Verify all records exist
	if memStore.Len() != 4 {
		t.Errorf("Expected 4 records, got %d", memStore.Len())
	}

	// Verify each record
	for _, m := range messages {
		record, _ := memStore.Get(ctx, m.ip, m.port, m.service)
		if record == nil {
			t.Errorf("Record not found for %s:%d/%s", m.ip, m.port, m.service)
		} else if record.Response != m.response {
			t.Errorf("Wrong response for %s:%d/%s: expected '%s', got '%s'",
				m.ip, m.port, m.service, m.response, record.Response)
		}
	}
}
