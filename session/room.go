package session

import (
	"encoding/json"
	"log/slog"

	"github.com/gorilla/websocket"
)

// Room delivers: it keeps the set of clients, passes their commands to the Sim,
// and sends every state the Sim produces to all of them. It doesn't run the sim.
// Only the Run goroutine touches clients; everyone else talks to it through
// the channels below, so the map needs no mutex.
type Room struct {
	sim     *Sim
	clients map[*Client]struct{} // a set: we only care whether a client is in it

	join     chan *Client // a new client arrived (Serve sends)
	leave    chan *Client // a client's readLoop ended (readLoop sends)
	commands chan []byte  // a command from a client (readLoop sends); forwarded to the sim
}

func NewRoom(sim *Sim) *Room {
	return &Room{
		sim:      sim,
		clients:  make(map[*Client]struct{}),
		join:     make(chan *Client),
		leave:    make(chan *Client),
		commands: make(chan []byte),
	}
}

// Serve runs one WebSocket connection inside the room until it disconnects.
// The HTTP handler calls it after the upgrade, so the handler needs no session internals.
func (r *Room) Serve(conn *websocket.Conn) {
	c := NewClient(r, conn)
	r.join <- c      // hand the new client to Run; waits until Run has taken it
	go c.writeLoop() // writing: its own new goroutine
	c.readLoop()     // reading: this (the handler's) goroutine, until the client leaves
}

// Run is the room's own goroutine: main starts it once with `go room.Run()`.
// It handles one event at a time, which is why the map is safe without a lock.
func (r *Room) Run() {
	for {
		select {
		case newClient := <-r.join:
			r.clients[newClient] = struct{}{} // add to the set; only this goroutine touches the map

		case removeClient := <-r.leave:
			// sendAll may have dropped this client already (too slow), and closing
			// a channel twice panics. So only act if it's still in the set.
			// ! (value, bool) = when declaring against map[]
			_, ok := r.clients[removeClient]
			if ok {
				r.drop(removeClient)
			}

		case cmd := <-r.commands:
			r.sim.commands <- cmd // IN: hand it to the sim (buffered, so this doesn't wait)

		case state := <-r.sim.output:
			// OUT: a new state from the sim. Encode once, send the same bytes to everyone.
			data, err := json.Marshal(state) // ! struct → JSON bytes
			if err != nil {
				slog.Error("encode state failed", "err", err)
				continue // ! skip to the next loop round; nothing to send
			}
			r.sendAll(data)
		}
	}
}

// ----- UTILS -----
// sendAll queues data for every client without waiting on any of them.
// Only Run calls it.
func (r *Room) sendAll(data []byte) {
	// ! range over a map gives each key; here each key is a client.
	for c := range r.clients {
		// ! select with default: try the send, but never wait.
		select {
		case c.send <- data: // room in c's buffer: queued for its writeLoop
		default: // buffer full: c is too slow, drop it instead of stalling everyone
			r.drop(c) // ! deleting inside range over a map is allowed in Go
		}
	}
}

// drop removes a client and closes its send channel, which ends its writeLoop.
// Callers make sure c is still in the set, so send is never closed twice.
func (r *Room) drop(c *Client) {
	delete(r.clients, c) // ! go's built-in to delete from a map
	close(c.send)        // ! go's built-in to close a channel
}
