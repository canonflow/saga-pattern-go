package model

type Order struct {
	ID         int64   `json:"id" gorm:"column:id,primaryKey"`
	UUID       string  `json:"uuid" gorm:"column:uuid"`
	CustomerID string  `json:"customer_id" gorm:"column:customer_id"`
	Item       string  `json:"item" gorm:"column:item"`
	Quantity   int     `json:"quantity" gorm:"column:quantity"`
	Amount     float32 `json:"amount" gorm:"column:amount"`
}
