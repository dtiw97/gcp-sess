package session

import (
	"encoding/json"
	"gcp-sess/machine"
	"log/slog"
	"time"

	"github.com/gorilla/websocket"
)

// writeWait is the longest one write may take; a stalled client fails instead of hanging.
const writeWait = 5 * time.Second

// sendBuffer is how many outgoing messages can queue up for one client.
// The room drops a message in and moves on, instead of waiting on a slow network.
const sendBuffer = 16

// Client is one WebSocket connection in a room.
// Room.Serve starts it: writeLoop in a new goroutine, readLoop in the current one.
type Client struct {
	room *Room // the shared room this client is in: readLoop calls room.engine.Apply and room.Leave
	conn *websocket.Conn
	send chan []byte   // outgoing messages; the room puts them in, writeLoop takes them out
	done chan struct{} // closed when writeLoop finishes; closeAll waits on it
}

// NewClient takes the room from the caller: every client must point at the SAME room.
func NewClient(room *Room, conn *websocket.Conn) *Client {
	return &Client{
		room: room,
		conn: conn,
		send: make(chan []byte, sendBuffer),
		done: make(chan struct{}), // must be made: a nil channel panics on close
	}
}

// readLoop takes inputs from the browser and hands them to the engine.
// It never broadcasts: the tick loop is the only thing that sends to clients.
// It runs until the browser leaves or the connection breaks.
func (c *Client) readLoop() {
	// Registered first, so cleanup happens however the loop ends.
	defer func() {
		c.room.Leave(c) // the room removes us and closes c.send, which ends writeLoop
		c.conn.Close()
	}()

	for {
		_, data, err := c.conn.ReadMessage() // waits here until the browser sends something
		if err != nil {
			// Usually just a normal goodbye (tab closed), not a failure.
			slog.Info("ws disconnected", "remote", c.conn.RemoteAddr().String(), "err", err)
			return
		}

		// ! JSON bytes → struct. &input: a pointer, so Unmarshal fills OUR variable.
		var input machine.Input
		if err := json.Unmarshal(data, &input); err != nil {
			// One bad message isn't a reason to disconnect: log, skip, keep the last good input.
			slog.Warn("bad input", "remote", c.conn.RemoteAddr().String(), "err", err)
			continue
		}
		c.room.engine.Apply(input) // IN: takes effect on the next tick, for everyone
	}
}

// writeLoop writes c.send to the browser; the only writer on c.conn (gorilla allows one).
// Each client has its own, so a slow network blocks only this goroutine, for at most writeWait.
func (c *Client) writeLoop() {
	defer close(c.done) // runs last (defers run in reverse): signals after the conn is closed
	// Closing the conn also makes readLoop's ReadMessage fail, so both loops end together.
	defer c.conn.Close()

	// range waits for the next message and ends when the room closes c.send.
	for msg := range c.send {
		c.conn.SetWriteDeadline(time.Now().Add(writeWait)) // ! a moment on the clock: reset before every write
		// TextMessage: the browser receives a string (we'll send JSON).
		err := c.conn.WriteMessage(websocket.TextMessage, msg)
		if err != nil {
			// The browser is gone; writing more is pointless.
			slog.Info("ws write failed", "remote", c.conn.RemoteAddr().String(), "err", err)
			return
		}
	}

	// c.send closed (shutdown, leave or dropped): say goodbye with 1001; an empty body arrives as 1005.
	c.conn.SetWriteDeadline(time.Now().Add(writeWait))
	c.conn.WriteMessage(
		websocket.CloseMessage,
		websocket.FormatCloseMessage(websocket.CloseGoingAway, "session closed"),
	)
}
