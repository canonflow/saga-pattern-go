package delivery

import (
	"orchestration/internal/handler"

	"github.com/gin-gonic/gin"
)

type RouteConfig struct {
	Router       *gin.RouterGroup
	OrderHandler *handler.OrderHandler
}

func (c *RouteConfig) Setup() {
	OrderRoute(c.Router, c.OrderHandler)
}
