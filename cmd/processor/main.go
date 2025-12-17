package main

import (
	"context"
	"log"
	"os"
	"os/signal"
	"syscall"

	"github.com/censys/scan-takehome/pkg/processor"
	"github.com/censys/scan-takehome/pkg/store"
)

func main() {
	// Get configuration from environment variables
	projectID := getEnv("PUBSUB_PROJECT_ID", "test-project")
	subscriptionID := getEnv("PUBSUB_SUBSCRIPTION_ID", "scan-sub")
	storeType := getEnv("STORE_TYPE", "sqlite")
	storeConnection := getEnv("STORE_CONNECTION", "/data/scans.db")

	log.Printf("starting processor with config:")
	log.Printf("  project ID: %s", projectID)
	log.Printf("  subscription ID: %s", subscriptionID)
	log.Printf("  store type: %s", storeType)
	log.Printf("  store connection: %s", storeConnection)

	// Create store
	s, err := store.NewStore(storeType, storeConnection)
	if err != nil {
		log.Fatalf("failed to create store: %v", err)
	}
	defer s.Close()
	log.Printf("store initialized successfully")

	// Create processor
	proc := processor.NewProcessor(s)

	// Create context that cancels on SIGINT/SIGTERM
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Handle shutdown signals
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)
	go func() {
		sig := <-sigChan
		log.Printf("received signal %v, shutting down...", sig)
		cancel()
	}()

	// Create and start consumer
	consumer, err := processor.NewConsumer(ctx, projectID, subscriptionID, proc)
	if err != nil {
		log.Fatalf("failed to create consumer: %v", err)
	}
	defer consumer.Close()
	log.Printf("consumer initialized successfully")

	// Start consuming messages (blocks until context is canceled)
	log.Printf("starting to consume messages...")
	if err := consumer.Start(ctx); err != nil {
		log.Fatalf("consumer error: %v", err)
	}

	log.Printf("processor shut down gracefully")
}

// getEnv returns the value of an environment variable or a default value
func getEnv(key, defaultValue string) string {
	if value := os.Getenv(key); value != "" {
		return value
	}
	return defaultValue
}
