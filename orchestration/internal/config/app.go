package config

import (
	"os"

	"orchestration/internal/delivery"
	"orchestration/internal/handler"
	"orchestration/internal/repository"
	usecase "orchestration/internal/usecase/order"

	"github.com/gin-gonic/gin"
	"gorm.io/gorm"
)

type BootstrapConfig struct {
	DB   *gorm.DB
	App  *gin.Engine
	Saga usecase.SagaStarter
}

func Bootstrap(config *BootstrapConfig) {
	driver := os.Getenv("DB_DRIVER")

	// Order domain
	orderRepository := repository.OrderRepositoryFactory(driver, config.DB)
	orderUsecase := usecase.NewOrderUsecase(orderRepository, config.Saga)
	orderHandler := handler.NewOrderHandler(orderUsecase)

	// Versioned API group: /api/v1
	v1 := config.App.Group("/api/v1")

	route := delivery.RouteConfig{
		Router:       v1,
		OrderHandler: orderHandler,
	}
	route.Setup()
}
