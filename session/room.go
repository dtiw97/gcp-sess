package session

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
		case c := <-r.join:
			_ = c
			// TODO(you): add c to r.clients

		case c := <-r.leave:
			_ = c
			// TODO(you):
			//   1. if c is still in r.clients: delete it, then close(c.send)
			//      (closing ends c's writeLoop; check first so we never close twice)

		case msg := <-r.broadcast:
			_ = msg
			// TODO(you): for each client c in r.clients:
			//   select {
			//   case c.send <- msg: // queued
			//   default:            // c.send is full: client too slow, drop it
			//       delete it from r.clients and close(c.send)
			//   }
		}
	}
}
