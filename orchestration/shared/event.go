package shared

import (
	"context"
	"encoding/json"
	"log"
	"time"

	"github.com/IBM/sarama"
	"github.com/hashicorp/go-uuid"
)

type (
	EventType   string
	CommandType string
	ReplyType   string
)

// Commands — sent by the orchestrator TO services
const (
	CmdProcessPayment  CommandType = "cmd.process_payment"
	CmdRefundPayment   CommandType = "cmd.refund_payment"
	CmdArrangeShipping CommandType = "cmd.reserve_inventory"
)

// Replies — sent by services back TO the orchestrator
const (
	ReplyPaymentSuccess   ReplyType = "reply.payment.success"
	ReplyPaymentFailed    ReplyType = "reply.payment.failed"
	ReplyRefundSuccess    ReplyType = "reply.refund.success"
	ReplyShippingArranged ReplyType = "reply.inventory.arranged"
	ReplyShippingFailed   ReplyType = "reply.inventory.failed"
)

// Kafka topics
const (
	TopicPaymentCmd    = "payment.commands"
	TopicPaymentReply  = "payment.replies"
	TopicShippingCmd   = "shipping.commands"
	TopicShippingReply = "shipping.replies"
)

type Message struct {
	ID        string          `json:"id"`
	Type      string          `json:"event"`
	OrderID   string          `json:"order_id"`
	Timestamp time.Time       `json:"timestamp"`
	Data      json.RawMessage `json:"data"`
}

// NewMessage builds a Message with a generated ID and current timestamp.
func NewMessage(msgType, orderID string, data json.RawMessage) Message {
	id, _ := uuid.GenerateUUID()
	return Message{
		ID:        id,
		Type:      msgType,
		OrderID:   orderID,
		Timestamp: time.Now(),
		Data:      data,
	}
}

// Publish marshals and sends a Message to the given topic.
// The message is keyed by OrderID so all events for one order land on the
// same partition, preserving per-order ordering.
func Publish(producer sarama.SyncProducer, topic string, msg Message) error {
	payload, err := json.Marshal(msg)
	if err != nil {
		return err
	}

	_, _, err = producer.SendMessage(&sarama.ProducerMessage{
		Topic: topic,
		Key:   sarama.StringEncoder(msg.OrderID),
		Value: sarama.ByteEncoder(payload),
	})
	return err
}

type OrderData struct {
	CustomerID string  `json:"customer_id"`
	Item       string  `json:"item"`
	Quantity   int     `json:"quantity"`
	Amount     float64 `json:"amount"`
}

type ConsumerGroupHandler struct {
	Handler ConsumerContract
}

func (h *ConsumerGroupHandler) Setup(sarama.ConsumerGroupSession) error {
	return nil
}

func (h *ConsumerGroupHandler) Cleanup(sarama.ConsumerGroupSession) error {
	return nil
}

func (h *ConsumerGroupHandler) ConsumeClaim(session sarama.ConsumerGroupSession, claim sarama.ConsumerGroupClaim) error {
	for {
		select {
		case message := <-claim.Messages():
			if message == nil {
				return nil
			}

			// Handler
			err := h.Handler.Consume(message)

			if err != nil {
				log.Printf("[Event - ConsumeClaim] Failed to process message with error: %s\n", err.Error())
				return err
			} else {
				session.MarkMessage(message, "")
			}
		case <-session.Context().Done():
			return nil
		}
	}
}

func ConsumeTopic(ctx context.Context, consumerGroup sarama.ConsumerGroup, topics []string, handler ConsumerContract) {
	consumerHandler := &ConsumerGroupHandler{
		Handler: handler,
	}

	go func() {
		for {
			if err := consumerGroup.Consume(
				ctx, topics, consumerHandler,
			); err != nil {
				log.Printf("[Event - Consume Topic] Error consume: %s\n", err)
			}

			if ctx.Err() != nil {
				log.Printf("Context Cancelled, stopping consumer!\n")
				return
			}
		}
	}()

	go func() {
		for err := range consumerGroup.Errors() {
			log.Printf("[Event - Consume Topic] Error from consumer: %s\n", err)
		}
	}()
	<-ctx.Done()

	log.Printf("Closing consumer group for topic\n")
	if err := consumerGroup.Close(); err != nil {
		log.Printf("[Event - Consume Topic] Error closing consume group: %s\n", err)
	}
}
