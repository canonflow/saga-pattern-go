package main

import (
	"context"
	"fmt"
	"log"
	"os"
	"os/signal"
	"syscall"

	"choreography/internal/config"
	"choreography/internal/shared"

	"github.com/IBM/sarama"
	"github.com/google/uuid"
	"github.com/joho/godotenv"
)

func main() {
	err := godotenv.Load()
	if err != nil {
		panic(err)
	}
	
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Listen Compensation
	orderHandler := &OrderHandler{}
	consumerGroup, err := config.GetKafkaConsumer("order_consumer")
	if err != nil {
		log.Fatalf("[Order Service] Error init consume group: %s\n", err)
	}

	go shared.ConsumeTopic(
		ctx,
		consumerGroup,
		[]string{shared.TopicPayments},
		orderHandler,
	)

	// Send Order
	createOrder()

	// Graceful Shutdown
	sig := make(chan os.Signal, 1)
	signal.Notify(sig, syscall.SIGINT, syscall.SIGTERM)
	<-sig

	consumerGroup.Close()
	fmt.Println("\n[Order Service] shutting down")
}

func createOrder() {
	orderId := "ORD-" + uuid.NewString()

	orderData := shared.OrderData{
		CustomerID: "CUST-" + uuid.NewString(),
	}

	eventBytes, err := shared.NewEvent(
		shared.OrderCreated,
		orderId,
		orderData,
	)
	if err != nil {
		log.Fatalf("[Order Service] Failed to create event: %v", err)
	}

	broker, err := config.GetKafkaProducer("order_producer")

	message := &sarama.ProducerMessage{
		Topic: shared.TopicOrders,
		Key:   sarama.StringEncoder(orderId),
		Value: sarama.ByteEncoder(eventBytes),
	}

	partition, offset, err := broker.SendMessage(message)
	if err != nil {
		log.Fatalf("[Order Service] Failed to publish a message", err)
	}

	log.Println("Message Send to topic %s, partition %d, offset %d", shared.TopicOrders, partition, offset)
}

type OrderHandler struct{}

func (h *OrderHandler) Consume(message *sarama.ConsumerMessage) error {
	event, err := shared.ParseEvent(message.Value)
	if err != nil {
		return err
	}

	// Process The Event
	switch event.Type {
	case shared.PaymentFailed, shared.PaymentRefunded:
		log.Printf("[Order Service - Consume] Received %s - Cancelling order %s\\n", event.Type, event.OrderID)
	}

	return nil
}
