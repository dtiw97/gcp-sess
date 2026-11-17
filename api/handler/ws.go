package handler

import (
	"errors"
	"gcp-sess/repo"
	"gcp-sess/session"
	"log/slog"
	"net/http"

	"github.com/gin-gonic/gin"
	"github.com/gorilla/websocket"
)

type WS struct {
	upgrader websocket.Upgrader
	sessions *session.Manager
}

func NewWS(sessions *session.Manager) *WS {
	return &WS{
		upgrader: websocket.Upgrader{},
		sessions: sessions,
	}
}

// Serve: GET /sessions/:id/ws. Gets the room running that session (resuming it from the DB), then upgrades.
func (h *WS) Serve(c *gin.Context) {
	id := c.Param("id")

	// Look the room up BEFORE upgrading: until then we can still answer with a
	// normal HTTP status; after the 101 the connection is a WebSocket.
	room, err := h.sessions.Room(c.Request.Context(), id)
	switch {
	case errors.Is(err, repo.ErrNotFound):
		c.JSON(http.StatusNotFound, gin.H{"message": "session not found"})
		return
	case errors.Is(err, session.ErrBusy):
		c.JSON(http.StatusConflict, gin.H{"message": "another session is live"})
		return
	case errors.Is(err, session.ErrClosed):
		c.JSON(http.StatusServiceUnavailable, gin.H{"message": "shutting down"})
		return
	case err != nil:
		slog.Error("load session failed", "session", id, "err", err)
		c.JSON(http.StatusInternalServerError, gin.H{"message": "could not load session"})
		return
	}

	conn, err := h.upgrader.Upgrade(c.Writer, c.Request, nil)
	if err != nil { // error check
		slog.Warn("websocket upgrade failed", "err", err)
		return // conn is nil here; without return, room.Serve(nil) below would panic
	}
	// No defer conn.Close(): the session (client's read/write loops) closes conn.
	slog.Info("ws connected", "session", id, "client_ip", c.ClientIP())
	// Hand the connection to the session's room. Blocks until the client leaves.
	room.Serve(conn)
}
