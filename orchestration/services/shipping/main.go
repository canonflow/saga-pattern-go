package shipping

import (
	"context"
	"encoding/json"
	"log"
	"math/rand"
	"time"

	"orchestration/internal/config"
	"orchestration/shared"

	"github.com/IBM/sarama"
)

const consumerGroup = "shipping-service"

// ShippingService simulates a distributed shipping participant in the saga.
type ShippingService struct {
	producer sarama.SyncProducer
}

// Start boots the shipping service: it consumes shipping commands and replies
// with the outcome. It blocks until ctx is cancelled.
func Start(ctx context.Context) error {
	consumer, err := config.GetKafkaConsumer(consumerGroup)
	if err != nil {
		return err
	}

	producer, err := config.GetKafkaProducer(consumerGroup)
	if err != nil {
		return err
	}

	svc := &ShippingService{producer: producer}

	log.Printf("[Shipping] listening on %s", shared.TopicShippingCmd)
	shared.ConsumeTopic(ctx, consumer, []string{shared.TopicShippingCmd}, svc)
	return nil
}

// Consume implements shared.ConsumerContract.
func (s *ShippingService) Consume(message *sarama.ConsumerMessage) error {
	var msg shared.Message
	if err := json.Unmarshal(message.Value, &msg); err != nil {
		log.Printf("[Shipping] failed to decode message: %v", err)
		return err
	}

	switch shared.CommandType(msg.Type) {
	case shared.CmdArrangeShipping:
		return s.arrangeShipping(msg)
	default:
		log.Printf("[Shipping] ignoring unknown command: %s", msg.Type)
		return nil
	}
}

func (s *ShippingService) arrangeShipping(msg shared.Message) error {
	var data shared.OrderData
	_ = json.Unmarshal(msg.Data, &data)

	log.Printf("[Shipping] arranging shipping for order %s (item: %s, qty: %d)", msg.OrderID, data.Item, data.Quantity)
	simulateWork()

	// Simulate a business failure: out of stock for large quantities.
	if data.Quantity > 10 {
		log.Printf("[Shipping] FAILED for order %s: out of stock", msg.OrderID)
		return s.reply(msg, string(shared.ReplyShippingFailed))
	}

	log.Printf("[Shipping] ARRANGED for order %s", msg.OrderID)
	return s.reply(msg, string(shared.ReplyShippingArranged))
}

func (s *ShippingService) reply(origin shared.Message, replyType string) error {
	reply := shared.NewMessage(replyType, origin.OrderID, origin.Data)
	return shared.Publish(s.producer, shared.TopicShippingReply, reply)
}

func simulateWork() {
	time.Sleep(time.Duration(rand.Intn(400)+100) * time.Millisecond)
}
