package main

import (
	"context"
	"log"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"orchestration/internal/config"
	"orchestration/internal/orchestrator"
	"orchestration/services/payment"
	"orchestration/services/shipping"

	"github.com/joho/godotenv"
)

// startDistributed simulates the Payment and Shipping services (distributed system).
// Each runs as a Kafka consumer/producer in its own goroutine.
func startDistributed(ctx context.Context) {
	go func() {
		if err := payment.Start(ctx); err != nil {
			log.Printf("[Payment] service error: %v", err)
		}
	}()

	go func() {
		if err := shipping.Start(ctx); err != nil {
			log.Printf("[Shipping] service error: %v", err)
		}
	}()
}

func main() {
	if err := godotenv.Load(); err != nil {
		log.Println("No .env file found, relying on environment variables")
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Core dependencies
	db := config.NewDatabase()
	app := config.NewGin()

	// Saga orchestrator: consumes replies and drives order status.
	orch, err := orchestrator.NewOrchestrator(db)
	if err != nil {
		log.Fatalf("Failed to init orchestrator: %v", err)
	}
	go orch.StartSaga(ctx)

	// Wire all domains (repositories, usecases, handlers, routes)
	config.Bootstrap(&config.BootstrapConfig{
		DB:   db,
		App:  app,
		Saga: orch,
	})

	// Simulate distributed payment/shipping services
	startDistributed(ctx)

	// HTTP server
	port := os.Getenv("APP_PORT")
	if port == "" {
		port = "8080"
	}

	server := &http.Server{
		Addr:    ":" + port,
		Handler: app,
	}

	go func() {
		log.Printf("Server listening on :%s", port)
		if err := server.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			log.Fatalf("Failed to start server: %v", err)
		}
	}()

	// Graceful shutdown
	quit := make(chan os.Signal, 1)
	signal.Notify(quit, syscall.SIGINT, syscall.SIGTERM)
	<-quit
	log.Println("Shutting down server ...")

	shutdownCtx, cancelServer := context.WithTimeout(ctx, 5*time.Second)
	defer cancelServer()

	if err := server.Shutdown(shutdownCtx); err != nil {
		log.Fatalf("Server forced to shutdown: %v", err)
	}

	// Stop consumers and close Kafka connections
	cancel()
	config.CloseKafka()

	log.Println("Server exiting")
}
