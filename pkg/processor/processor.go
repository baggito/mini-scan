package processor

import (
	"context"
	"encoding/json"
	"fmt"
	"log"

	"cloud.google.com/go/pubsub"
	"github.com/censys/scan-takehome/pkg/scanning"
	"github.com/censys/scan-takehome/pkg/store"
)

// rawScan is used for JSON unmarshalling with json.RawMessage for the Data field
type rawScan struct {
	IP          string          `json:"ip"`
	Port        uint32          `json:"port"`
	Service     string          `json:"service"`
	Timestamp   int64           `json:"timestamp"`
	DataVersion int             `json:"data_version"`
	Data        json.RawMessage `json:"data"`
}

// Processor handles scan message processing
type Processor struct {
	store store.Store
}

// NewProcessor creates a new processor with the given store
func NewProcessor(s store.Store) *Processor {
	return &Processor{store: s}
}

// Process processes a single scan message
func (p *Processor) Process(ctx context.Context, data []byte) error {
	// Parse the scan message
	scan, response, err := p.parseScan(data)
	if err != nil {
		return fmt.Errorf("failed to parse scan: %w", err)
	}

	// Create service record
	record := &store.ServiceRecord{
		IP:            scan.Ip,
		Port:          scan.Port,
		Service:       scan.Service,
		LastTimestamp: scan.Timestamp,
		Response:      response,
	}

	// Upsert to store (handles out-of-order messages via timestamp comparison)
	updated, err := p.store.Upsert(ctx, record)
	if err != nil {
		return fmt.Errorf("failed to upsert record: %w", err)
	}

	if updated {
		log.Printf("updated record: ip=%s port=%d service=%s timestamp=%d",
			scan.Ip, scan.Port, scan.Service, scan.Timestamp)
	} else {
		log.Printf("skipped older record: ip=%s port=%d service=%s timestamp=%d",
			scan.Ip, scan.Port, scan.Service, scan.Timestamp)
	}

	return nil
}

// parseScan parses a scan message and extracts the response string
func (p *Processor) parseScan(data []byte) (*scanning.Scan, string, error) {
	var raw rawScan
	if err := json.Unmarshal(data, &raw); err != nil {
		return nil, "", fmt.Errorf("failed to unmarshal scan: %w", err)
	}

	var response string
	switch raw.DataVersion {
	case scanning.V1:
		var v1 scanning.V1Data
		if err := json.Unmarshal(raw.Data, &v1); err != nil {
			return nil, "", fmt.Errorf("failed to unmarshal V1 data: %w", err)
		}
		// Go's json.Unmarshal automatically decodes base64 into []byte
		response = string(v1.ResponseBytesUtf8)

	case scanning.V2:
		var v2 scanning.V2Data
		if err := json.Unmarshal(raw.Data, &v2); err != nil {
			return nil, "", fmt.Errorf("failed to unmarshal V2 data: %w", err)
		}
		response = v2.ResponseStr

	default:
		return nil, "", fmt.Errorf("unknown data version: %d", raw.DataVersion)
	}

	scan := &scanning.Scan{
		Ip:          raw.IP,
		Port:        raw.Port,
		Service:     raw.Service,
		Timestamp:   raw.Timestamp,
		DataVersion: raw.DataVersion,
	}

	return scan, response, nil
}

// Consumer handles Pub/Sub message consumption
type Consumer struct {
	client       *pubsub.Client
	subscription *pubsub.Subscription
	processor    *Processor
}

// NewConsumer creates a new Pub/Sub consumer
func NewConsumer(ctx context.Context, projectID, subscriptionID string, processor *Processor) (*Consumer, error) {
	client, err := pubsub.NewClient(ctx, projectID)
	if err != nil {
		return nil, fmt.Errorf("failed to create pubsub client: %w", err)
	}

	sub := client.Subscription(subscriptionID)

	// Check if subscription exists
	exists, err := sub.Exists(ctx)
	if err != nil {
		client.Close()
		return nil, fmt.Errorf("failed to check subscription existence: %w", err)
	}
	if !exists {
		client.Close()
		return nil, fmt.Errorf("subscription %s does not exist", subscriptionID)
	}

	return &Consumer{
		client:       client,
		subscription: sub,
		processor:    processor,
	}, nil
}

// Start starts consuming messages from the subscription
// This method blocks until the context is cancelled
func (c *Consumer) Start(ctx context.Context) error {
	log.Printf("starting to consume messages from subscription: %s", c.subscription.ID())

	err := c.subscription.Receive(ctx, func(ctx context.Context, msg *pubsub.Message) {
		// Process the message
		if err := c.processor.Process(ctx, msg.Data); err != nil {
			log.Printf("failed to process message: %v", err)
			// NACK the message so it will be redelivered
			msg.Nack()
			return
		}

		// ACK only after successful processing (at-least-once semantics)
		msg.Ack()
	})

	if err != nil && ctx.Err() == nil {
		return fmt.Errorf("subscription receive error: %w", err)
	}

	return nil
}

// Close closes the Pub/Sub client
func (c *Consumer) Close() error {
	return c.client.Close()
}
