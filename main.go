package main

import (
	"context"
	"errors"
	"gcp-sess/handler"
	"gcp-sess/routes"
	"gcp-sess/utils/loggers"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/gin-gonic/gin"
)

func main() {
	loggers.Setup()
	router := gin.Default()
	router.GET("/", func(c *gin.Context) {
		time.Sleep(5 * time.Second) // simulate a slow in-flight request, to test shutdown later
		// To simulate case A, where request takes longer than required to shutdown.
		// time.Sleep(15 * time.Second)
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

	// ! App Context detects SIGTERM for termination
	appCtx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	go func() {
		// Serve Server
		if err := server.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			slog.Error("serving failed", "addr", server.Addr, "err", err)
			os.Exit(1)
		}
	}()
	health.SetReady(true)
	<-appCtx.Done() // Channels sleeps main thread until it receives from channel.
	stop()          // Receives signal, proceed
	health.SetReady(false)

	// ! ShutDown context, sets a context of 10s
	shutDownCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	start := time.Now() // Current timestamp.

	// Server shutdown with shutDownCtx
	// 1. Immediately stop accepting new connections.
	// 2. Waits for request already in flight to finish.
	if err := server.Shutdown(shutDownCtx); err != nil {
		if errors.Is(err, context.DeadlineExceeded) {
			// case A: hit 10s, requests still running → they get cut off
			slog.Warn("shutdown timed out, active requests cut off", "after", time.Since(start).String())
		} else {
			// case B: some other failure (rare, eg: closing the listener failed)
			slog.Error("shutdown failed", "after", time.Since(start).String(), "err", err)
		}
		return // don't fall through to "complete": requests were not all finished
	}

	slog.Info("server shut down complete", "after", time.Since(start).String())
}
