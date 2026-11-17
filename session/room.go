package session

import (
	"context"
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
	id      string // the session id (for logs and saving)
	mu      sync.Mutex
	clients map[*Client]struct{} // a set: we only care whether a client is in it
	closed  bool                 // set by closeAll: Join refuses, so nobody joins a room that stopped ticking
	engine  *Engine
}

func NewRoom(id string, engine *Engine) *Room {
	return &Room{
		id:      id,
		clients: make(map[*Client]struct{}),
		engine:  engine,
	}
}

// Serve runs one WebSocket connection inside the room until it disconnects.
// The HTTP handler calls it after the upgrade, so the handler needs no session internals.
// It runs on the handler's goroutine and returns when the client leaves.
func (r *Room) Serve(conn *websocket.Conn) {
	c := NewClient(r, conn) // ! Every client gets the same *Room
	// add to the set first, so the first Broadcast already reaches c
	if !r.Join(c) {
		// The room already stopped (shutdown): say goodbye straight away.
		close(c.send)
		c.writeLoop() // nothing queued: sends 1001 and closes conn
		return
	}

	// ! Client's write and read goroutines.
	go c.writeLoop() // writing: its own new goroutine
	c.readLoop()     // reading: this (the handler's) goroutine, until the client leaves
}

// Run is the tick loop: each tick it steps the machine and broadcasts; returns when ctx is cancelled.
func (r *Room) Run(ctx context.Context) {
	// ! ticker.C is a channel; Go puts a value in it every tickInterval, forever.
	ticker := time.NewTicker(tickInterval)
	defer ticker.Stop() // turn the timer off when Run returns

	// ! select waits on both channels; whichever is ready first runs.
	for {
		select {
		case <-ctx.Done():
			// ! return, not break: break would only leave the select.
			r.closeAll()
			return

		case <-ticker.C: // ! <- receives: "when the next tick arrives"
			// Two locks, never held together: no lock-ordering deadlock.
			state := r.engine.Step(tickInterval) // engine lock on/off; returns a copy
			data, err := json.Marshal(state)     // no lock held: state is our own copy
			if err != nil {
				slog.Error("encode state failed", "err", err)
				continue // ! skip to the next tick; nothing to send
			}
			r.Broadcast(data) // room lock on/off; encoded once, same bytes to everyone
		} // end select
	} // end for
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
			slog.Warn("client too slow, dropped", "session", r.id, "remote", c.conn.RemoteAddr().String())
			close(c.send) // ends c's writeLoop
		}
	}
}

// -------- Join / Leave / closeAll ---------

// Len is how many clients are connected. Manager uses it: a room with clients isn't switched.
func (r *Room) Len() int {
	r.mu.Lock()
	defer r.mu.Unlock()
	return len(r.clients)
}

// Join adds a client and reports whether it was let in. Called by Serve.
func (r *Room) Join(c *Client) bool {
	r.mu.Lock()         // wait until no other goroutine is inside the room
	defer r.mu.Unlock() // let the next one in when Join returns

	if r.closed {
		return false
	}
	r.clients[c] = struct{}{} // add c to the set
	slog.Info("client joined", "session", r.id, "remote", c.conn.RemoteAddr().String())
	return true
}

// Leave removes a client. Called by readLoop when the connection ends.
func (r *Room) Leave(c *Client) {
	r.mu.Lock()         // take the key (or wait for it)
	defer r.mu.Unlock() // give it back when Leave returns

	// Broadcast or closeAll may have removed c already; closing a channel twice panics.
	// ! (value, bool) lookup: ok says whether c is still in the set.
	_, ok := r.clients[c]
	if ok {
		delete(r.clients, c)
		slog.Info("client left", "session", r.id, "remote", c.conn.RemoteAddr().String())
		close(c.send) // ends c's writeLoop
	}
}

// closeAll ends every client, then waits until each has sent its close frame.
func (r *Room) closeAll() {
	r.mu.Lock()
	r.closed = true
	gone := make([]*Client, 0, len(r.clients))
	for c := range r.clients {
		delete(r.clients, c) // Leave later finds ok == false: no double close
		close(c.send)        // ends writeLoop
		gone = append(gone, c)
	}
	r.mu.Unlock() // not defer: never wait while holding the lock

	for _, c := range gone {
		<-c.done // wait for this client's writeLoop to finish
	}
	slog.Info("all clients closed", "session", r.id, "count", len(gone))
}
