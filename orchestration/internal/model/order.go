package model

import "time"

type Order struct {
	ID         int       `json:"id" gorm:"column:id;primaryKey;autoIncrement"`
	CustomerID string    `json:"customer_id" gorm:"column:customer_id"`
	Item       string    `json:"item" gorm:"column:item"`
	Quantity   int       `json:"quantity" gorm:"column:quantity"`
	Amount     float32   `json:"amount" gorm:"column:amount"`
	Status     string    `json:"status" gorm:"column:status"`
	CreatedAt  time.Time `json:"created_at" gorm:"column:created_at;autoCreateTime"`
	UpdatedAt  time.Time `json:"updated_at" gorm:"column:updated_at;autoUpdateTime"`
}
