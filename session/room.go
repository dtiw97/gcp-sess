package session

import (
	"context"
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

	// done is closed when Run returns. Senders select on it so they never wait
	// forever on a Room that has stopped (nobody would receive their send).
	done chan struct{}
}

func NewRoom(sim *Sim) *Room {
	return &Room{
		sim:      sim,
		clients:  make(map[*Client]struct{}),
		join:     make(chan *Client),
		leave:    make(chan *Client),
		commands: make(chan []byte),
		done:     make(chan struct{}), // ! must be made: closing a nil channel panics
	}
}

// Serve runs one WebSocket connection inside the room until it disconnects.
// The HTTP handler calls it after the upgrade, so the handler needs no session internals.
func (r *Room) Serve(conn *websocket.Conn) {
	c := NewClient(r, conn)
	// Hand the new client to Run, unless the room has already stopped.
	select {
	case r.join <- c: // Run took it
	case <-r.done: // room stopped: nobody will ever take it, so don't wait
		conn.Close()
		return
	}
	go c.writeLoop() // writing: its own new goroutine
	c.readLoop()     // reading: this (the handler's) goroutine, until the client leaves
}

// Run is the room's own goroutine: main starts it once with `wg.Go(func() { room.Run(sessCtx) })`.
// It handles one event at a time, which is why the map is safe without a lock.
// It returns when ctx is cancelled, after closing every client.
func (r *Room) Run(ctx context.Context) {
	// Registered first, so it runs however Run ends: tells every sender "stop waiting on me".
	defer close(r.done)

	for {
		select {
		case <-ctx.Done():
			// Session is over: say goodbye to every client.
			// ! []*Client is a slice (a growable list). drop deletes from the map,
			// so keep the clients here to wait on them afterwards.
			var gone []*Client
			// 1. Start every goodbye at once: drop only closes channels, it never waits.
			for client := range r.clients {
				gone = append(gone, client) // ! append returns the list with one more item
				r.drop(client)              // delete + close(c.send) → its writeLoop sends the 1001 close frame
			}
			// 2. Then wait for each writeLoop. They ran in parallel, so this takes
			// about as long as the slowest client, not the sum of all of them.
			// ! range over a slice gives (index, value); _ skips the index.
			for _, client := range gone {
				<-client.done // blocks until that writeLoop has closed done
			}
			slog.Info("room stopped")
			return // ! ends Run; from now on nobody receives on join/leave/commands

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
