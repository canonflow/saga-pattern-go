package dto

type CreateOrder struct {
	CustomerID string  `json:"customer_id"`
	Item       string  `json:"item"`
	Quantity   int     `json:"quantity"`
	Amount     float32 `json:"amount"`
}

type UpdateOrder struct {
	Status string `json:"status"`
}
