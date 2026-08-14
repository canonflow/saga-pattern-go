package payment

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

const consumerGroup = "payment-service"

// PaymentService simulates a distributed payment participant in the saga.
type PaymentService struct {
	producer sarama.SyncProducer
}

// Start boots the payment service: it consumes payment commands and replies
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

	svc := &PaymentService{producer: producer}

	log.Printf("[Payment] listening on %s", shared.TopicPaymentCmd)
	shared.ConsumeTopic(ctx, consumer, []string{shared.TopicPaymentCmd}, svc)
	return nil
}

// Consume implements shared.ConsumerContract.
func (s *PaymentService) Consume(message *sarama.ConsumerMessage) error {
	var msg shared.Message
	if err := json.Unmarshal(message.Value, &msg); err != nil {
		log.Printf("[Payment] failed to decode message: %v", err)
		return err
	}

	switch shared.CommandType(msg.Type) {
	case shared.CmdProcessPayment:
		return s.processPayment(msg)
	case shared.CmdRefundPayment:
		return s.refundPayment(msg)
	default:
		log.Printf("[Payment] ignoring unknown command: %s", msg.Type)
		return nil
	}
}

func (s *PaymentService) processPayment(msg shared.Message) error {
	var data shared.OrderData
	_ = json.Unmarshal(msg.Data, &data)

	log.Printf("[Payment] processing payment for order %s (amount: %.2f)", msg.OrderID, data.Amount)
	simulateWork()

	// Simulate a business failure: insufficient funds for large amounts.
	if data.Amount > 5000 {
		log.Printf("[Payment] FAILED for order %s: insufficient funds", msg.OrderID)
		return s.reply(msg, string(shared.ReplyPaymentFailed))
	}

	log.Printf("[Payment] SUCCESS for order %s", msg.OrderID)
	return s.reply(msg, string(shared.ReplyPaymentSuccess))
}

// refundPayment is the compensating action, triggered when a later saga step fails.
func (s *PaymentService) refundPayment(msg shared.Message) error {
	log.Printf("[Payment] refunding payment for order %s", msg.OrderID)
	simulateWork()

	log.Printf("[Payment] refund complete for order %s", msg.OrderID)
	return s.reply(msg, string(shared.ReplyRefundSuccess))
}

func (s *PaymentService) reply(origin shared.Message, replyType string) error {
	reply := shared.NewMessage(replyType, origin.OrderID, origin.Data)
	return shared.Publish(s.producer, shared.TopicPaymentReply, reply)
}

func simulateWork() {
	time.Sleep(time.Duration(rand.Intn(400)+100) * time.Millisecond)
}
