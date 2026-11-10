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
	send chan []byte   // outgoing messages; the room puts them in, writeLoop takes them out
	done chan struct{} // closed when writeLoop has finished (close frame sent); the room waits on it
}

func NewClient(room *Room, conn *websocket.Conn) *Client {
	return &Client{
		room: room,
		conn: conn,
		send: make(chan []byte, sendBuffer),
		done: make(chan struct{}), // ! must be made: closing a nil channel panics
	}
}

// readLoop takes messages from the browser and hands them to the room.
// It runs until the browser leaves or the connection breaks.
func (c *Client) readLoop() {
	// Registered first, so cleanup happens however the loop ends.
	defer func() {
		// ! select with two cases: whichever can happen first wins.
		select {
		case c.room.leave <- c: // room running: it removes us and closes c.send, which ends writeLoop
		case <-c.room.done: // room stopped: it already closed c.send; nobody to tell
		}
		c.conn.Close()
	}()

	for {
		_, data, err := c.conn.ReadMessage() // waits here until the browser sends something
		if err != nil {
			// Usually just a normal goodbye (tab closed), not a failure.
			slog.Info("ws disconnected", "remote", c.conn.RemoteAddr().String(), "err", err)
			return
		}
		// A command for the room; everyone sees its effect on the next tick.
		select {
		case c.room.commands <- data: // room running: handed over
		case <-c.room.done: // room stopped: the command has nowhere to go
			return // runs the defer above
		}
	}
}

// writeLoop takes messages from c.send and writes them to the browser.
// It is the only code that writes to c.conn: gorilla allows one writer per connection.
//
// WriteMessage can wait a long time on a slow network. Because each client has its
// own writeLoop, only that client's goroutine waits; the room and other clients don't.
func (c *Client) writeLoop() {
	// ! Defers run in reverse order: conn.Close() first, then close(c.done).
	// So "done" is signalled only after everything else, close frame included.
	defer close(c.done)
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
	// ! The close frame's body is a 2-byte code + reason text. 1001 "going away" =
	// server shutting down; an empty body shows in the browser as 1005 "no status".
	// (Slow clients dropped by sendAll get this too, though it isn't a shutdown. Fine for now.)
	c.conn.WriteMessage(
		websocket.CloseMessage,
		websocket.FormatCloseMessage(websocket.CloseGoingAway,
			"server shutting down"))
}
