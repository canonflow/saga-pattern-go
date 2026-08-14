package usecase

import (
	"log"

	"orchestration/internal/dto"
	"orchestration/internal/model"
	repository "orchestration/internal/repository/order"
)

type OrderImpl struct {
	orderRepository repository.IOrderRepository
	saga            SagaStarter
}

func NewOrderUsecase(orderRepository repository.IOrderRepository, saga SagaStarter) *OrderImpl {
	return &OrderImpl{
		orderRepository: orderRepository,
		saga:            saga,
	}
}

func (u *OrderImpl) GetOrdersByCustomerID(customerId string) ([]model.Order, error) {
	return u.orderRepository.GetOrderByCustomerID(customerId)
}

func (u *OrderImpl) GetOrderByID(orderId int64) (model.Order, error) {
	return u.orderRepository.GetOrderByID(orderId)
}

func (u *OrderImpl) CreateOrder(payload dto.CreateOrder) (model.Order, error) {
	order, err := u.orderRepository.Create(payload)
	if err != nil {
		return model.Order{}, err
	}

	// Kick off the saga (process payment -> arrange shipping).
	if u.saga != nil {
		if err := u.saga.StartOrderSaga(order); err != nil {
			log.Printf("[Order] failed to start saga for order %d: %v", order.ID, err)
		}
	}

	return order, nil
}

func (u *OrderImpl) UpdateOrder(orderId int64, payload dto.UpdateOrder) (model.Order, error) {
	return u.orderRepository.Update(orderId, payload)
}
