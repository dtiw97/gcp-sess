package session

import (
	"encoding/json"
	"log/slog"
	"sync"
	"time"

	"github.com/gorilla/websocket"
)

// Room is one session: a set of clients that all receive the machine's state each tick.
// Many goroutines call its methods at once (each handler, each readLoop, the tick loop),
// so every access to clients happens while holding mu.
type Room struct {
	mu      sync.Mutex
	clients map[*Client]struct{} // a set: we only care whether a client is in it
	engine  *Engine
}

func NewRoom(engine *Engine) *Room {
	return &Room{
		clients: make(map[*Client]struct{}),
		engine:  engine,
	}
}

// Serve runs one WebSocket connection inside the room until it disconnects.
// The HTTP handler calls it after the upgrade, so the handler needs no session internals.
// It runs on the handler's goroutine and returns when the client leaves.
func (r *Room) Serve(conn *websocket.Conn) {
	c := NewClient(r, conn) // ! r is this room: every client gets the same *Room
	r.Join(c)               // add to the set first, so the first Broadcast already reaches c
	go c.writeLoop()        // writing: its own new goroutine
	c.readLoop()            // reading: this (the handler's) goroutine, until the client leaves
}

// Run is the tick loop (tbm-sim's broadcastLoop): main starts it once with `go room.Run()`.
// Every tickInterval it steps the machine and sends the new state to every client.
// It never returns (2.4 adds a way to stop it).
func (r *Room) Run() {
	// ! ticker.C is a channel; Go puts a value in it every tickInterval, forever.
	ticker := time.NewTicker(tickInterval)
	defer ticker.Stop() // turn the timer off when Run returns

	// ! range over a channel: run the body once per value; a ticker's channel is
	// never closed, so this loops for the life of the server.
	for range ticker.C {
		// Two locks, never held together: engine's in Step, room's in Broadcast.
		// So no lock-ordering deadlock is possible.
		state := r.engine.Step(tickInterval) // engine lock on/off; returns a copy
		data, err := json.Marshal(state)     // no lock held: state is our own copy
		if err != nil {
			slog.Error("encode state failed", "err", err)
			continue // ! skip to the next tick; nothing to send
		}
		r.Broadcast(data) // room lock on/off; encoded once, same bytes to everyone
	}
}

// Join adds a client. Called by Serve.
func (r *Room) Join(c *Client) {
	r.mu.Lock()         // wait until no other goroutine is inside the room
	defer r.mu.Unlock() // let the next one in when Join returns

	r.clients[c] = struct{}{} // add c to the set
}

// Leave removes a client. Called by readLoop when the connection ends.
func (r *Room) Leave(c *Client) {
	r.mu.Lock()         // take the key (or wait for it)
	defer r.mu.Unlock() // give it back when Leave returns

	// Broadcast may have dropped c already (too slow), and closing a channel
	// twice panics. So only act if c is still in the set.
	// Check and close both happen under the lock: nobody can close in between.
	// ! No loop needed: a map lookup by key finds c directly.
	// ! (value, bool): the bool says whether the key was found
	_, ok := r.clients[c]
	if ok {
		delete(r.clients, c) // ! go's built-in to delete from a map
		close(c.send)        // ends c's writeLoop (its range over c.send stops)
	}
}

// Broadcast queues msg for every client. Called by the tick loop (Run) each tick.
// It never waits on a client: the lock must only be held for quick work.
func (r *Room) Broadcast(msg []byte) {
	r.mu.Lock()
	defer r.mu.Unlock()

	// ! range over a map with one variable gives each key: here each client.
	for c := range r.clients {
		// ! select with default: try the send, but never wait (we hold the lock).
		select {
		case c.send <- msg: // the case itself is the send: queued for c's writeLoop
		default: // c.send is full: c is too slow, drop it instead of stalling everyone
			// No ok check: c comes from the map and we hold the lock, so it's in the set.
			// Not r.Leave(c): Leave locks again, and a Go mutex isn't re-entrant (deadlock).
			delete(r.clients, c) // ! deleting inside range over a map is allowed
			close(c.send)        // ends c's writeLoop
		}
	}
}
