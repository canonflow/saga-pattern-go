package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"math/rand/v2"
	"os"
	"os/signal"
	"syscall"

	"choreography/internal/config"
	"choreography/internal/shared"

	"github.com/IBM/sarama"
	"github.com/joho/godotenv"
)

type ShippingHandler struct {
	Producer sarama.SyncProducer
}

func (h *ShippingHandler) Consume(message *sarama.ConsumerMessage) error {
	event, err := shared.ParseEvent(message.Value)
	if err != nil {
		return err
	}

	// Process the event
	if event.Type == shared.PaymentSucceeded {
		processShipping(event, h.Producer)
	}

	return nil
}

func main() {
	err := godotenv.Load()
	if err != nil {
		panic(err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Init producer
	shippingProducer, err := config.GetKafkaProducer("shipping_producer")
	if err != nil {
		log.Fatalf("[Shipping Service] Error init consume group: %s\n", err)
	}

	// Init Consumer
	shippingHandler := &ShippingHandler{
		Producer: shippingProducer,
	}
	consumerGroup, err := config.GetKafkaConsumer("shipping_consumer")
	if err != nil {
		log.Fatalf("[Shipping Service] Error init consume group: %s\n", err)
	}

	go shared.ConsumeTopic(
		ctx,
		consumerGroup,
		[]string{shared.TopicPayments},
		shippingHandler,
	)

	// Graceful Shutdown
	sig := make(chan os.Signal, 1)
	signal.Notify(sig, syscall.SIGINT, syscall.SIGTERM)
	<-sig

	// Close both producer and consumer
	shippingProducer.Close()
	consumerGroup.Close()

	fmt.Println("\n[Payment Service] shutting down")
}

func processShipping(event *shared.Event, producer sarama.SyncProducer) {
	var order shared.OrderData

	_ = json.Unmarshal(event.Data, &order)
	log.Printf("[Shipping Service] Process Shipping for order %s\n", event.OrderID)

	// Simulate: 80% success, 20% failure
	success := rand.Float32() < 0.8

	var eventType shared.EventType
	if success {
		eventType = shared.ShippingArranged
		log.Printf("[Shipping Service] shipping arranged successfully for order %s\n", event.OrderID)
	} else {
		eventType = shared.ShippingFailed
		log.Printf("[Payment Service] shipping FAILED for order %\ns", event.OrderID)
	}

	eventBytes, err := shared.NewEvent(
		eventType,
		event.OrderID,
		order,
	)
	if err != nil {
		log.Fatalf("[Shipping Service] Failed to create event: %v\n", err)
	}

	message := &sarama.ProducerMessage{
		Topic: shared.TopicShipping,
		Key:   sarama.StringEncoder(event.OrderID),
		Value: sarama.ByteEncoder(eventBytes),
	}

	partition, offset, err := producer.SendMessage(message)
	if err != nil {
		log.Fatalf("[Payment Service] Failed to publish a message\n", err)
	}

	log.Println("[Payment Service] Message Send to topic %s, partition %d, offset %d", shared.TopicPayments, partition, offset)
}
