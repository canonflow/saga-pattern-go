package model

import (
	"encoding/json"
	"time"
)

type EventMessage struct {
	ID        int       `json:"id" gorm:"column:id;primaryKey;autoIncrement"`
	Type      string    `json:"type" gorm:"column:type"`
	OrderID   int       `json:"order_id" gorm:"column:order_id"`
	Data      string    `json:"data" gorm:"column:data"`
	CreatedAt time.Time `json:"created_at" gorm:"column:created_at;autoCreateTime"`
	UpdatedAt time.Time `json:"updated_at" gorm:"column:updated_at;autoUpdateTime"`
}

func (m *EventMessage) ParseData() (map[string]interface{}, error) {
	var result map[string]interface{}

	if err := json.Unmarshal([]byte(m.Data), &result); err != nil {
		return nil, err
	}

	return result, nil
}
