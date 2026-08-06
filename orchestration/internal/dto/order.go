package dto

type CreateOrder struct {
	Item     string  `json:"item"`
	Amount   int     `json:"amount"`
	Quantity float32 `json:"quantity"`
}
