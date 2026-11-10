package handler

import (
	"gcp-sess/session"
	"log/slog"

	"github.com/gin-gonic/gin"
	"github.com/gorilla/websocket"
)

type WS struct {
	upgrader websocket.Upgrader
	room     *session.Room
}

func NewWS(room *session.Room) *WS {
	return &WS{
		upgrader: websocket.Upgrader{},
		room:     room,
	}
}

func (h *WS) Serve(c *gin.Context) {
	conn, err := h.upgrader.Upgrade(c.Writer, c.Request, nil)
	if err != nil { // error check
		slog.Warn("websocket upgrade failed", "err", err)
		return // conn is nil here; without return, room.Serve(nil) below would panic
	}
	// No defer conn.Close(): the session (client's read/write loops) closes conn.
	slog.Info("ws connected", "client_ip", c.ClientIP())
	// Hand the connection to the shared room (created in main). Blocks until the client leaves.
	h.room.Serve(conn)
}
