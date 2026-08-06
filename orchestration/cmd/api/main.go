package main

import (
	"context"
	"log"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/joho/godotenv"
)

// Start Payment and Shipping Process (Simulate the distributed system)
func startDistributed()

func main() {
	err := godotenv.Load()
	if err != nil {
		panic(err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	go startDistributed()

	// Graceful shutdown
	quit := make(chan os.Signal, 1)
	signal.Notify(quit, syscall.SIGINT, syscall.SIGTERM)
	<-quit
	log.Println("Shutdown Server ...")

	// Shutdown server
	_, cancelServer := context.WithTimeout(ctx, 5*time.Second)
	defer cancelServer()

	// if err := server.Shutdown(ctx); err != nil {
	// 	cancelQueue()
	// 	logrus.Infoln("Server Shutdown:", err)
	// }
	// logrus.Infoln("Server exiting")
}
