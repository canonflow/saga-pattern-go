package orchestrator

import (
	"context"
	"encoding/json"
	"log"
	"os"
	"strconv"

	"orchestration/internal/config"
	"orchestration/internal/dto"
	"orchestration/internal/model"
	"orchestration/internal/repository"
	orderRepository "orchestration/internal/repository/order"
	"orchestration/shared"

	"github.com/IBM/sarama"
	"gorm.io/gorm"
)

// Order status lifecycle driven by the saga.
const (
	StatusPending   = "PENDING"
	StatusPaid      = "PAID"
	StatusCompleted = "COMPLETED"
	StatusCancelled = "CANCELLED"
)

// Orchestrator coordinates the saga: it sends commands to the payment and
// shipping services, reacts to their replies, and updates order status.
type Orchestrator struct {
	consumer  sarama.ConsumerGroup
	producer  sarama.SyncProducer
	orderRepo orderRepository.IOrderRepository
}

func NewOrchestrator(db *gorm.DB) (*Orchestrator, error) {
	consumer, err := config.GetKafkaConsumer("orchestrator")
	if err != nil {
		return nil, err
	}

	producer, err := config.GetKafkaProducer("orchestrator")
	if err != nil {
		return nil, err
	}

	repo := repository.OrderRepositoryFactory(os.Getenv("DB_DRIVER"), db)

	return &Orchestrator{
		consumer:  consumer,
		producer:  producer,
		orderRepo: repo,
	}, nil
}

// StartSaga begins consuming reply topics. It blocks until ctx is cancelled.
func (o *Orchestrator) StartSaga(ctx context.Context) {
	log.Printf("[Orchestrator] listening on %s and %s", shared.TopicPaymentReply, shared.TopicShippingReply)
	shared.ConsumeTopic(ctx, o.consumer, []string{shared.TopicPaymentReply, shared.TopicShippingReply}, o)
}

// StartOrderSaga is the entry point: kick off the saga for a newly created order
// by asking the payment service to process payment.
func (o *Orchestrator) StartOrderSaga(order model.Order) error {
	data := shared.OrderData{
		CustomerID: order.CustomerID,
		Item:       order.Item,
		Quantity:   order.Quantity,
		Amount:     float64(order.Amount),
	}

	payload, err := json.Marshal(data)
	if err != nil {
		return err
	}

	msg := shared.NewMessage(string(shared.CmdProcessPayment), strconv.Itoa(order.ID), payload)
	return o.sendCommand(shared.TopicPaymentCmd, msg)
}

// Consume implements shared.ConsumerContract: it handles replies from services.
func (o *Orchestrator) Consume(message *sarama.ConsumerMessage) error {
	var msg shared.Message
	if err := json.Unmarshal(message.Value, &msg); err != nil {
		log.Printf("[Orchestrator] failed to decode reply: %v", err)
		return err
	}

	log.Printf("[Orchestrator] received %s for order %s", msg.Type, msg.OrderID)

	switch shared.ReplyType(msg.Type) {
	case shared.ReplyPaymentSuccess:
		// Payment done -> mark PAID and ask shipping to proceed.
		o.updateStatus(msg.OrderID, StatusPaid)
		next := shared.NewMessage(string(shared.CmdArrangeShipping), msg.OrderID, msg.Data)
		return o.sendCommand(shared.TopicShippingCmd, next)

	case shared.ReplyPaymentFailed:
		// Payment failed -> saga ends, order cancelled.
		o.updateStatus(msg.OrderID, StatusCancelled)

	case shared.ReplyShippingArranged:
		// Final step succeeded -> order completed.
		o.updateStatus(msg.OrderID, StatusCompleted)

	case shared.ReplyShippingFailed:
		// Shipping failed -> compensate by refunding the payment.
		refund := shared.NewMessage(string(shared.CmdRefundPayment), msg.OrderID, msg.Data)
		return o.sendCommand(shared.TopicPaymentCmd, refund)

	case shared.ReplyRefundSuccess:
		// Compensation done -> order cancelled.
		o.updateStatus(msg.OrderID, StatusCancelled)

	default:
		log.Printf("[Orchestrator] ignoring unknown reply: %s", msg.Type)
	}

	return nil
}

func (o *Orchestrator) sendCommand(topic string, msg shared.Message) error {
	log.Printf("[Orchestrator] sending %s for order %s -> %s", msg.Type, msg.OrderID, topic)
	return shared.Publish(o.producer, topic, msg)
}

func (o *Orchestrator) updateStatus(orderID, status string) {
	id, err := strconv.ParseInt(orderID, 10, 64)
	if err != nil {
		log.Printf("[Orchestrator] invalid order id %q: %v", orderID, err)
		return
	}

	if _, err := o.orderRepo.Update(id, dto.UpdateOrder{Status: status}); err != nil {
		log.Printf("[Orchestrator] failed to update order %s to %s: %v", orderID, status, err)
		return
	}

	log.Printf("[Orchestrator] order %s -> %s", orderID, status)
}
