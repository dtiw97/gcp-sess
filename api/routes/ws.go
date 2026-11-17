package routes

import (
	"gcp-sess/api/handler"

	"github.com/gin-gonic/gin"
)

func WSRoutes(r gin.IRouter, h *handler.WS) {
	r.GET("/sessions/:id/ws", h.Serve)
}
