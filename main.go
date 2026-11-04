package main

import (
	"errors"
	"gcp-sess/handler"
	"gcp-sess/routes"
	"log"
	"net/http"
	"time"

	"github.com/gin-gonic/gin"
)

func main() {
	router := gin.Default()
	router.GET("/", func(c *gin.Context) {
		time.Sleep(5 * time.Second) // simulate a slow in-flight request, to test shutdown later
		c.JSON(http.StatusOK, gin.H{
			"message": "home",
		})
	})

	health := &handler.HealthHandler{}
	routes.HealthRoutes(router, health)

	// Setup server
	server := &http.Server{
		Addr:    ":8080",
		Handler: router,
	}

	// Serve Server
	if err := server.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
		log.Fatal(err)
	}
}
