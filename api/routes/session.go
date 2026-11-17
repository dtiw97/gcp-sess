package routes

import (
	"gcp-sess/api/handler"

	"github.com/gin-gonic/gin"
)

func SessionRoutes(r gin.IRouter, h *handler.SessionHandler) {
	r.POST("/sessions", h.Create)
	r.GET("/sessions", h.List)
	r.GET("/sessions/:id", h.Get)
}
