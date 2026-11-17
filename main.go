package main

import (
	"context"
	"errors"
	h "gcp-sess/api/handler"
	"gcp-sess/api/middleware"
	r "gcp-sess/api/routes"
	"gcp-sess/repo"
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

	// Open the database before serving anything: no DB → no sessions → fail at startup.
	store, err := repo.Open(cfg.DBPath)
	if err != nil {
		slog.Error("open database failed", "path", cfg.DBPath, "err", err)
		os.Exit(1)
	}
	defer store.Close() // runs last, after the final save

	health := &h.HealthHandler{}
	r.HealthRoutes(router, health)

	// One room for the whole server (tbm-sim: maxRooms=1). The DB holds every session;
	// GET /sessions/:id/ws loads one into the room (if the room has no clients on another).

	// * Session context: its own root (Background), NOT appCtx. SIGTERM must not stop
	// the room; it keeps ticking through the drain. Only stopSess() (at the end) does.
	sessCtx, stopSess := context.WithCancel(context.Background())
	sessions := session.NewManager(sessCtx, store) // owns the one Room

	// The save loop: every 3 s it writes the live session to the DB; on shutdown it
	// stops the room (clients get 1001), then saves once more. wg.Go = go + counted.
	var wg sync.WaitGroup
	wg.Go(sessions.Run)
	r.SessionRoutes(router, h.NewSessionHandler(store))
	r.WSRoutes(router, h.NewWS(sessions))

	// Dummy frontend (web/index.html): served from this server so the page's origin
	// matches /ws, which the Upgrader requires. Open http://localhost:8080/ui
	router.StaticFile("/ui", "./web/index.html")

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
	slog.Info("Readiness false. App shutting down. Draining", "delay", cfg.DrainDelay.String())
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
	} else {
		slog.Info("server shut down complete", "after", time.Since(start).String())
	}
	// No return above: the session must stop on BOTH paths, or clients get 1006.

	// * Stop the session last: no new clients can arrive now (Shutdown closed the door).
	stopSess() // cancels sessCtx → Manager.Run stops the room (closeAll) → final save
	wg.Wait()  // main waits for Manager.Run; if main returned first, clients would get 1006
	slog.Info("session stopped")
}
