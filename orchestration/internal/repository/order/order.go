package repository

import (
	"orchestration/internal/dto"
	"orchestration/internal/model"
)

type IOrderRepository interface {
	GetOrderByCustomerID(customerId string) ([]model.Order, error)
	GetOrderByID(orderId int64) (model.Order, error)
	Create(payload dto.CreateOrder) (model.Order, error)
	Update(orderId int64, payload dto.UpdateOrder) (model.Order, error)
}
