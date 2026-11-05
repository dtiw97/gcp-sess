package session

import (
	"log/slog"

	"github.com/gorilla/websocket"
)

// sendBuffer is how many outgoing messages can queue up for one client.
// The room drops a message in and moves on, instead of waiting on a slow network.
const sendBuffer = 16

// Client is one WebSocket connection in a room.
// Room.Serve starts it: writeLoop in a new goroutine, readLoop in the current one.
type Client struct {
	room *Room // the room this client belongs to: where readLoop sends messages and "I left"
	conn *websocket.Conn
	send chan []byte // outgoing messages; the room puts them in, writeLoop takes them out
}

func NewClient(room *Room, conn *websocket.Conn) *Client {
	return &Client{
		room: room,
		conn: conn,
		send: make(chan []byte, sendBuffer),
	}
}

// readLoop takes messages from the browser and hands them to the room.
// It runs until the browser leaves or the connection breaks.
func (c *Client) readLoop() {
	// Registered first, so cleanup happens however the loop ends.
	defer func() {
		c.room.leave <- c // the room removes us and closes c.send, which ends writeLoop
		c.conn.Close()
	}()

	for {
		_, data, err := c.conn.ReadMessage() // waits here until the browser sends something
		if err != nil {
			// Usually just a normal goodbye (tab closed), not a failure.
			slog.Info("ws disconnected", "remote", c.conn.RemoteAddr().String(), "err", err)
			return
		}
		c.room.broadcast <- data // the room passes it to every client, including us
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
