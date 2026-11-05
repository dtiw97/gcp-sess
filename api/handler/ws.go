package handler

import (
	"log/slog"

	"github.com/gin-gonic/gin"
	"github.com/gorilla/websocket"
)

type WS struct {
	upgrader websocket.Upgrader
}

func NewWS() *WS {
	return &WS{
		upgrader: websocket.Upgrader{},
	}
}

func (h *WS) Serve(c *gin.Context) {
	conn, err := h.upgrader.Upgrade(c.Writer, c.Request, nil)
	if err != nil { // error check
		slog.Warn("websocket upgrade failed", "err", err)
		return // conn is nil here; without return, conn.Close() below panics
	}
	defer conn.Close() // cleanup.

	slog.Info("ws connected", "client_ip", c.ClientIP())

	// Echo loop: one goroutine both reads and writes. Fine for echo, because we
	// only write in reply to a read. The room (2.2) needs separate read and write loops.
	for {
		msgType, data, err := conn.ReadMessage() // blocks until the client sends something
		if err != nil {
			// Client closed the tab, sent a close frame, or the connection broke.
			slog.Info("ws disconnected", "client_ip", c.ClientIP(), "err", err)
			return
		}
		if err := conn.WriteMessage(msgType, data); err != nil {
			slog.Info("ws write failed", "client_ip", c.ClientIP(), "err", err)
			return
		}
	}
}
