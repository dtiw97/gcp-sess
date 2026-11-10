package main

import (
	"context"
	"errors"
	h "gcp-sess/api/handler"
	"gcp-sess/api/middleware"
	r "gcp-sess/api/routes"
	"gcp-sess/session"
	u "gcp-sess/utils"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"strconv"
	"sync"
	"syscall"
	"time"

	"github.com/gin-gonic/gin"
)

func main() {
	u.Setup()
	cfg, err := LoadConfig()
	if err != nil {
		slog.Error("invalid config", "err", err)
		os.Exit(1)
	}
	// gin.New() instead of gin.Default(): keep Recovery, swap Gin's text Logger for our JSON one.
	router := gin.New()
	router.Use(
		gin.Recovery(),
		middleware.RequestLogger())

	router.GET("/", func(c *gin.Context) {
		time.Sleep(5 * time.Second) // simulate a slow in-flight request, to test shutdown later
		// To simulate case A, where request takes longer than required to shutdown.
		// time.Sleep(15 * time.Second)
		c.JSON(http.StatusOK, gin.H{
			"message": "home",
		})
	})

	health := &h.HealthHandler{}
	r.HealthRoutes(router, health)

	// * Session context: NOT appCtx. The session must keep running through the drain,
	// so main cancels it itself with stopSession(), after server.Shutdown.
	sessCtx, stopSession := context.WithCancel(context.Background())
	defer stopSession() // safety net for early returns; the normal path calls it explicitly

	// One session per server, like tbm-sim (maxRooms=1): a Sim that ticks and a Room
	// that delivers. Each Run is its own goroutine.
	sim := session.NewSim()
	room := session.NewRoom(sim)

	// * WaitGroup: a counter so main can wait for the actors before exiting.
	// ! wg.Go = Add(1) + start the goroutine + Done() when the function returns.
	// ! func() { ... } is an anonymous function: wg.Go wants a func() with no arguments.
	var wg sync.WaitGroup               // ! zero value = counter at 0, ready to use
	wg.Go(func() { sim.Run(sessCtx) })  // counter 1
	wg.Go(func() { room.Run(sessCtx) }) // counter 2

	ws := h.NewWS(room)
	r.WSRoutes(router, ws)

	// Setup server
	server := &http.Server{
		Addr:    ":" + strconv.Itoa(cfg.Port),
		Handler: router,
	}

	// * App Context detects SIGTERM for termination
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

	<-appCtx.Done()        // * Channels sleeps main thread until it receives from channel.
	stop()                 // Receives signal, proceed
	health.SetReady(false) // Returns 503, kube LB stop sending.
	slog.Info("Draining", "delay", cfg.DrainDelay.String())
	time.Sleep(cfg.DrainDelay) // * Drain Delay

	// * ShutDown context, sets a context of ShutdownTimeout
	shutDownCtx, cancel := context.WithTimeout(context.Background(), cfg.ShutdownTimeout)
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

	// * No new connections now: stop the session. Closes sessCtx.Done(), so Room.Run
	// (closes clients with 1001) and Sim.Run return.
	stopSession()
	wg.Wait() // * blocks until both Run functions return (counter back to 0); then main may exit
	slog.Info("session stopped")
}
