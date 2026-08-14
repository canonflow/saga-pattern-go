package repository

import (
	"orchestration/internal/dto"
	"orchestration/internal/model"

	"gorm.io/gorm"
)

type orderRepository_MySQL struct {
	db *gorm.DB
}

func NewOrderRepository_MySQL(db *gorm.DB) *orderRepository_MySQL {
	return &orderRepository_MySQL{db: db}
}

func (r *orderRepository_MySQL) GetOrderByCustomerID(customerId string) ([]model.Order, error) {
	var orders []model.Order
	if err := r.db.Where("customer_id = ?", customerId).Find(&orders).Error; err != nil {
		return nil, err
	}
	return orders, nil
}

func (r *orderRepository_MySQL) GetOrderByID(orderId int64) (model.Order, error) {
	var order model.Order
	if err := r.db.First(&order, orderId).Error; err != nil {
		return model.Order{}, err
	}
	return order, nil
}

func (r *orderRepository_MySQL) Create(payload dto.CreateOrder) (model.Order, error) {
	order := model.Order{
		CustomerID: payload.CustomerID,
		Item:       payload.Item,
		Amount:     payload.Amount,
		Quantity:   payload.Quantity,
		Status:     "PENDING",
	}
	if err := r.db.Create(&order).Error; err != nil {
		return model.Order{}, err
	}
	return order, nil
}

func (r *orderRepository_MySQL) Update(orderId int64, payload dto.UpdateOrder) (model.Order, error) {
	var order model.Order
	if err := r.db.First(&order, orderId).Error; err != nil {
		return model.Order{}, err
	}

	order.Status = payload.Status
	if err := r.db.Save(&order).Error; err != nil {
		return model.Order{}, err
	}
	return order, nil
}
