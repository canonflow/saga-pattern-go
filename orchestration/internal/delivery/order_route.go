package delivery

import (
	"orchestration/internal/handler"

	"github.com/gin-gonic/gin"
)

func OrderRoute(rg *gin.RouterGroup, orderHandler *handler.OrderHandler) {
	orders := rg.Group("/orders")
	{
		orders.GET("", orderHandler.GetByCustomerID)
		orders.GET("/:id", orderHandler.GetByID)
		orders.POST("", orderHandler.Create)
		orders.PATCH("/:id", orderHandler.Update)
	}
}
