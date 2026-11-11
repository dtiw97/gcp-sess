package session

import (
	"encoding/json"
	"gcp-sess/machine"
	"log/slog"

	"github.com/gorilla/websocket"
)

// sendBuffer is how many outgoing messages can queue up for one client.
// The room drops a message in and moves on, instead of waiting on a slow network.
const sendBuffer = 16

// Client is one WebSocket connection in a room.
// Room.Serve starts it: writeLoop in a new goroutine, readLoop in the current one.
type Client struct {
	room *Room // the shared room this client is in: readLoop calls room.engine.Apply and room.Leave
	conn *websocket.Conn
	send chan []byte // outgoing messages; the room puts them in, writeLoop takes them out
}

// NewClient takes the room from the caller: every client must point at the SAME room.
func NewClient(room *Room, conn *websocket.Conn) *Client {
	return &Client{
		room: room,
		conn: conn,
		send: make(chan []byte, sendBuffer),
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

// writeLoop takes messages from c.send and writes them to the browser.
// It is the only code that writes to c.conn: gorilla allows one writer per connection.
//
// WriteMessage can wait a long time on a slow network. Because each client has its
// own writeLoop, only that client's goroutine waits; the room and other clients don't.
func (c *Client) writeLoop() {
	// Closing the conn also makes readLoop's ReadMessage fail, so both loops end together.
	defer c.conn.Close()

	// range waits for the next message and ends when the room closes c.send.
	for msg := range c.send {
		// TextMessage: the browser receives a string (we'll send JSON).
		err := c.conn.WriteMessage(websocket.TextMessage, msg)
		if err != nil {
			// The browser is gone; writing more is pointless.
			slog.Info("ws write failed", "remote", c.conn.RemoteAddr().String(), "err", err)
			return
		}
	}

	// The room closed c.send: tell the browser we're closing, instead of just dropping it.
	c.conn.WriteMessage(websocket.CloseMessage, []byte{})
}
