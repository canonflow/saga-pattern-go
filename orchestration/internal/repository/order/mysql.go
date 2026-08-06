package repository

import (
	"orchestration/internal/dto"
	"orchestration/internal/model"
)

type orderRepository_MySQL struct{}

func NewOrderRepository_MySQL() *orderRepository_MySQL {
	return &orderRepository_MySQL{}
}

func (r *orderRepository_MySQL) GetOrderByCustomerID(customerId string) []model.Order
func (r *orderRepository_MySQL) GetOrderByID(orderId int64) model.Order
func (r *orderRepository_MySQL) Create(payload dto.CreateOrder) model.Order
