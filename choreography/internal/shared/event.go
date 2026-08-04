package shared

import (
	"context"
	"encoding/json"
	"log"
	"time"

	"github.com/IBM/sarama"
)

type EventType string

const (
	// Happy Path
	OrderCreated     EventType = "order.created"
	PaymentSucceeded EventType = "payment.succeeded"
	ShippingArranged EventType = "shipping.arranged"

	// Compensation
	PaymentFailed   EventType = "payment.failed"
	PaymentRefunded EventType = "payment.refunded"
	ShippingFailed  EventType = "shipping.failed"
)

const (
	TopicOrders   = "orders"
	TopicPayments = "payments"
	TopicShipping = "shippings"
)

type Event struct {
	ID        string          `json:"id"`
	Type      EventType       `json:"event"`
	OrderID   string          `json:"order_id"`
	Timestamp time.Time       `json:"timestamp"`
	Data      json.RawMessage `json:"data"`
}

type OrderData struct {
	CustomerID string  `json:"customer_id"`
	Item       string  `json:"item"`
	Quantity   int     `json:"quantity"`
	Amount     float64 `json:"float"`
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
	go func() {
		for err := range consumerGroup.Close().Error() {
			log.Printf("[Event - Consume Topic] Error closing consume group: %s\n", err)
		}
	}()
}

func NewEvent(eventType EventType, orderID string, data interface{}) ([]byte, error) {
	var rawData json.RawMessage
	if data != nil {
		d, err := json.Marshal(data)
		if err != nil {
			return nil, err
		}

		rawData = d
	}

	event := Event{
		ID:        orderID + "-" + string(eventType),
		Type:      eventType,
		OrderID:   orderID,
		Timestamp: time.Now(),
		Data:      rawData,
	}

	return json.Marshal(event)
}

func ParseEvent(data []byte) (*Event, error) {
	var event Event
	err := json.Unmarshal(data, &event)

	return &event, err
}
