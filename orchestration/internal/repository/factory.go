package repository

import (
	"strings"

	orderRepository "orchestration/internal/repository/order"

	"gorm.io/gorm"
)

func OrderRepositoryFactory(database string, db *gorm.DB) orderRepository.IOrderRepository {
	database = strings.ToLower(database)

	switch database {
	case "mysql":
		return orderRepository.NewOrderRepository_MySQL(db)
	// PosgreSQL, MariaDB, etc.
	default:
		return orderRepository.NewOrderRepository_MySQL(db)
	}
}
