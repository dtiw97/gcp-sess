package handler

import (
	"gcp-sess/session"
	"log/slog"

	"github.com/gin-gonic/gin"
	"github.com/gorilla/websocket"
)

type WS struct {
	upgrader websocket.Upgrader
	room     *session.Room // every connection joins this one room
}

func NewWS(room *session.Room) *WS {
	return &WS{
		upgrader: websocket.Upgrader{},
		room:     room,
	}
}

// Serve does the HTTP part (the upgrade), then hands the connection to the room.
func (h *WS) Serve(c *gin.Context) {
	conn, err := h.upgrader.Upgrade(c.Writer, c.Request, nil)
	if err != nil { // error check
		slog.Warn("websocket upgrade failed", "err", err)
		return // conn is nil here, so there is nothing to hand over
	}

	slog.Info("ws connected", "client_ip", c.ClientIP())

	// Blocks until this client disconnects; the client's own loops close the conn.
	h.room.Serve(conn)
}
