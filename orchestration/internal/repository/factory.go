package repository

import (
	"strings"

	orderRepository "orchestration/internal/repository/order"
)

func OrderRepositoryFactory(database string) orderRepository.IOrderRepository {
	database = strings.ToLower(database)

	switch database {
	case "mysql":
		return orderRepository.NewOrderRepository_MySQL()
	// PosgreSQL, MariaDB, etc.
	default:
		return orderRepository.NewOrderRepository_MySQL()
	}
}
