package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"math/rand"
	"os"
	"os/signal"
	"syscall"

	"choreography/internal/config"
	"choreography/internal/shared"

	"github.com/IBM/sarama"
)

type PaymentHandler struct {
	Producer sarama.SyncProducer
}

func (h *PaymentHandler) Consume(message *sarama.ConsumerMessage) error {
	event, err := shared.ParseEvent(message.Value)
	if err != nil {
		return err
	}

	// Process the event
	if event.Type == shared.OrderCreated {
		processPayment(event, h.Producer)
	} else if event.Type == shared.ShippingFailed {
		refundPayment(event.OrderID, h.Producer)
	}

	return nil
}

func main() {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Listen Compensation From Shipping
	paymentProducer, err := config.GetKafkaProducer("payment_producer")
	if err != nil {
		log.Fatalf("[Order Service] Error init consume group: %s\n", err)
	}

	paymentHandler := &PaymentHandler{
		Producer: paymentProducer,
	}
	consumerGroup, err := config.GetKafkaConsumer("payment_consumer")
	if err != nil {
		log.Fatalf("[Order Service] Error init consume group: %s\n", err)
	}

	go shared.ConsumeTopic(
		ctx,
		consumerGroup,
		[]string{shared.TopicOrders, shared.TopicShipping},
		paymentHandler,
	)

	// Graceful Shutdown
	sig := make(chan os.Signal, 1)
	signal.Notify(sig, syscall.SIGINT, syscall.SIGTERM)
	<-sig
	fmt.Println("\n[Payment Service] shutting down")
}

func processPayment(event *shared.Event, producer sarama.SyncProducer) {
	var order shared.OrderData

	_ = json.Unmarshal(event.Data, &order)

	log.Printf("[Payment Service] Process Payment of $%.2f for order %s\n", order.Amount, event.OrderID)

	// Simulate: 80% success, 20% failure
	success := rand.Float32() < 0.8

	var eventType shared.EventType
	if success {
		eventType = shared.PaymentSucceeded
		log.Printf("[Payment Service] payment successful for order %s\n", event.OrderID)
	} else {
		eventType = shared.PaymentFailed
		log.Printf("[Payment Service] payment FAILED for order %s", event.OrderID)
	}

	// publish event
	eventBytes, err := shared.NewEvent(
		eventType,
		event.OrderID,
		order,
	)
	if err != nil {
		log.Fatalf("[Payment Service] Failed to create event: %v", err)
	}

	message := &sarama.ProducerMessage{
		Topic: shared.TopicPayments,
		Key:   sarama.StringEncoder(event.OrderID),
		Value: sarama.ByteEncoder(eventBytes),
	}

	partition, offset, err := producer.SendMessage(message)
	if err != nil {
		log.Fatalf("[Payment Service] Failed to publish a message", err)
	}

	log.Println("[Payment Service] Message Send to topic %s, partition %d, offset %d", shared.TopicPayments, partition, offset)
}

func refundPayment(orderID string, producer sarama.SyncProducer) {
	log.Printf("[Payment Service] refund issued for order %s (compensation)\n", orderID)

	// publish event
	data, err := shared.NewEvent(
		shared.PaymentRefunded,
		orderID,
		nil,
	)
	if err != nil {
		log.Fatalf("[Payment Service] Send Refund Payment Event failed due to %v\n", err)
	}

	eventBytes, err := shared.NewEvent(
		shared.PaymentRefunded,
		orderID,
		data,
	)
	if err != nil {
		log.Fatalf("[Payment Service] Send Refund Payment Event failed due to: %v", err)
	}

	message := &sarama.ProducerMessage{
		Topic: shared.TopicPayments,
		Key:   sarama.StringEncoder(orderID),
		Value: sarama.ByteEncoder(eventBytes),
	}

	partition, offset, err := producer.SendMessage(message)
	if err != nil {
		log.Fatalf("[Payment Service] Failed to publish a message", err)
	}

	log.Println("[Payment Service] Refund Message Send to topic %s, partition %d, offset %d", shared.TopicPayments, partition, offset)
}
