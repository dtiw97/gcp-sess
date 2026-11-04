package handler

import (
	"net/http"
	"sync/atomic"

	"github.com/gin-gonic/gin"
)

type HealthHandler struct {
	ready atomic.Bool
}

func (h *HealthHandler) SetReady(v bool) {
	h.ready.Store(v)
}
func (h *HealthHandler) Live(c *gin.Context) {
	c.JSON(http.StatusOK, gin.H{
		"message": "Live",
	})
}
func (h *HealthHandler) Ready(c *gin.Context) {
	c.JSON(http.StatusOK, gin.H{
		"message": "Ready",
	})
}
