package main

import (
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

	router.Run()
}
