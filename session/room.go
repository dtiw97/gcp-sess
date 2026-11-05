package session

import "github.com/gorilla/websocket"

// Room is one session: a set of clients that see each other's messages.
// Only the Run goroutine touches clients; everyone else talks to it through
// the channels below, so the map needs no mutex.
type Room struct {
	clients map[*Client]struct{} // a set: we only care whether a client is in it

	join      chan *Client // a new client arrived (the WebSocket handler sends)
	leave     chan *Client // a client's readLoop ended (readLoop sends)
	broadcast chan []byte  // a message for everyone (readLoop sends)
}

func NewRoom() *Room {
	return &Room{
		clients:   make(map[*Client]struct{}),
		join:      make(chan *Client),
		leave:     make(chan *Client),
		broadcast: make(chan []byte),
	}
}

// Run is the room's own goroutine: main starts it once with `go room.Run()`.
// It handles one event at a time, which is why the map is safe without a lock.
func (r *Room) Run() {
	for {
		select {
		case newClient := <-r.join:
			r.clients[newClient] = struct{}{} // add to the set; only this goroutine touches the map

		case removeClient := <-r.leave:
			// Broadcast may have dropped this client already (too slow), and closing
			// a channel twice panics. So only act if it's still in the set.
			// ! (value, bool) = when declaring against map[]
			_, ok := r.clients[removeClient]
			if ok {
				delete(r.clients, removeClient) // ! go's built in command to delete from maps[].
				close(removeClient.send)        // ! go's built-in command to close a chan[].
			}

		case msg := <-r.broadcast:
			// ! range over a map gives each key; here each key is a client.
			for c := range r.clients {
				// ! select with default: try the send, but never wait.
				select {
				case c.send <- msg: // room in c's buffer: queued for its writeLoop
				default: // buffer full: c is too slow, drop it instead of stalling everyone
					delete(r.clients, c) // ! deleting inside range over a map is allowed in Go
					close(c.send)
				}
			}
		}
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
