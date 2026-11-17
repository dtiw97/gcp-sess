package handler

import (
	"crypto/rand"
	"errors"
	"gcp-sess/repo"
	"log/slog"
	"net/http"

	"github.com/gin-gonic/gin"
)

// SessionHandler serves the session REST API: create, list, get.
// Reads come from the database, so a live session's state is up to 3 s old there;
// the WebSocket has the live one.
type SessionHandler struct {
	store *repo.SessionRepo
}

func NewSessionHandler(store *repo.SessionRepo) *SessionHandler {
	return &SessionHandler{store: store}
}

// Create: POST /sessions → 201 with the new session. The room starts on the first connect.
func (h *SessionHandler) Create(c *gin.Context) {
	// ! rand.Text(): 26 random base32 characters (128 bits). Unguessable, so the id
	// alone is enough to get back into your session (no auth yet).
	id := rand.Text()
	s, err := h.store.Create(c.Request.Context(), id)
	if err != nil {
		slog.Error("create session failed", "err", err)
		c.JSON(http.StatusInternalServerError, gin.H{"message": "could not create session"})
		return
	}
	slog.Info("session created", "session", id)
	c.JSON(http.StatusCreated, s)
}

// List: GET /sessions → every session, newest first.
func (h *SessionHandler) List(c *gin.Context) {
	list, err := h.store.List(c.Request.Context())
	if err != nil {
		slog.Error("list sessions failed", "err", err)
		c.JSON(http.StatusInternalServerError, gin.H{"message": "could not list sessions"})
		return
	}
	c.JSON(http.StatusOK, list)
}

// Get: GET /sessions/:id → one session, or 404.
func (h *SessionHandler) Get(c *gin.Context) {
	s, err := h.store.Get(c.Request.Context(), c.Param("id"))
	if errors.Is(err, repo.ErrNotFound) {
		c.JSON(http.StatusNotFound, gin.H{"message": "session not found"})
		return
	}
	if err != nil {
		slog.Error("get session failed", "err", err)
		c.JSON(http.StatusInternalServerError, gin.H{"message": "could not get session"})
		return
	}
	c.JSON(http.StatusOK, s)
}
