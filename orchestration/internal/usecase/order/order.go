package usecase

import (
	"orchestration/internal/dto"
	"orchestration/internal/model"
)

type IOrderUsecase interface {
	GetOrdersByCustomerID(customerId string) ([]model.Order, error)
	GetOrderByID(orderId int64) (model.Order, error)
	CreateOrder(payload dto.CreateOrder) (model.Order, error)
	UpdateOrder(orderId int64, payload dto.UpdateOrder) (model.Order, error)
}

// SagaStarter is implemented by the orchestrator to begin a saga for a new order.
// Defined here (not imported from orchestrator) to avoid an import cycle.
type SagaStarter interface {
	StartOrderSaga(order model.Order) error
}
