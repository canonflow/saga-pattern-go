package handler

import (
	"net/http"
	"strconv"

	"orchestration/internal/dto"
	usecase "orchestration/internal/usecase/order"

	"github.com/gin-gonic/gin"
)

type OrderHandler struct {
	orderUsecase usecase.IOrderUsecase
}

func NewOrderHandler(orderUsecase usecase.IOrderUsecase) *OrderHandler {
	return &OrderHandler{orderUsecase: orderUsecase}
}

// GET /orders?customer_id=xxx
func (h *OrderHandler) GetByCustomerID(ctx *gin.Context) {
	customerId := ctx.Query("customer_id")
	if customerId == "" {
		ctx.JSON(http.StatusBadRequest, gin.H{
			"status": false,
			"error":  "customer_id is required",
		})
		return
	}

	orders, err := h.orderUsecase.GetOrdersByCustomerID(customerId)
	if err != nil {
		ctx.JSON(http.StatusInternalServerError, gin.H{
			"status": false,
			"error":  err.Error(),
		})
		return
	}

	ctx.JSON(http.StatusOK, gin.H{
		"status": true,
		"data":   orders,
	})
}

// GET /orders/:id
func (h *OrderHandler) GetByID(ctx *gin.Context) {
	orderId, err := strconv.ParseInt(ctx.Param("id"), 10, 64)
	if err != nil {
		ctx.JSON(http.StatusBadRequest, gin.H{
			"status": false,
			"error":  "invalid order id",
		})
		return
	}

	order, err := h.orderUsecase.GetOrderByID(orderId)
	if err != nil {
		ctx.JSON(http.StatusNotFound, gin.H{
			"status": false,
			"error":  err.Error(),
		})
		return
	}

	ctx.JSON(http.StatusOK, gin.H{
		"status": true,
		"data":   order,
	})
}

// POST /orders
func (h *OrderHandler) Create(ctx *gin.Context) {
	var payload dto.CreateOrder
	if err := ctx.ShouldBindJSON(&payload); err != nil {
		ctx.JSON(http.StatusBadRequest, gin.H{
			"status": false,
			"error":  err.Error(),
		})
		return
	}

	order, err := h.orderUsecase.CreateOrder(payload)
	if err != nil {
		ctx.JSON(http.StatusInternalServerError, gin.H{
			"status": false,
			"error":  err.Error(),
		})
		return
	}

	ctx.JSON(http.StatusCreated, gin.H{
		"status": true,
		"data":   order,
	})
}

// PATCH /orders/:id
func (h *OrderHandler) Update(ctx *gin.Context) {
	orderId, err := strconv.ParseInt(ctx.Param("id"), 10, 64)
	if err != nil {
		ctx.JSON(http.StatusBadRequest, gin.H{
			"status": false,
			"error":  "invalid order id",
		})
		return
	}

	var payload dto.UpdateOrder
	if err := ctx.ShouldBindJSON(&payload); err != nil {
		ctx.JSON(http.StatusBadRequest, gin.H{
			"status": false,
			"error":  err.Error(),
		})
		return
	}

	order, err := h.orderUsecase.UpdateOrder(orderId, payload)
	if err != nil {
		ctx.JSON(http.StatusInternalServerError, gin.H{
			"status": false,
			"error":  err.Error(),
		})
		return
	}

	ctx.JSON(http.StatusOK, gin.H{
		"status": true,
		"data":   order,
	})
}
