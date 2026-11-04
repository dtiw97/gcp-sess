package routes

import (
	"gcp-sess/handler"

	"github.com/gin-gonic/gin"
)

func HealthRoutes(r gin.IRouter, h *handler.HealthHandler) {
	r.GET("/livez", h.Live)
	r.GET("/readyz", h.Ready)
}
