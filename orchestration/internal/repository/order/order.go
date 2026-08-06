package repository

import (
	"orchestration/internal/dto"
	"orchestration/internal/model"
)

type IOrderRepository interface {
	GetOrderByCustomerID(customerId string) []model.Order
	GetOrderByID(orderId int64) model.Order
	Create(payload dto.CreateOrder) model.Order
}
