# Module 2: WebSocket room + tick (mini tbm-sim)

Goal: turn the service into a **session server**: clients stay connected and share live state. This is what Agones will later manage, one session per pod.

## Lessons

| # | Lesson | Status | Commit |
|---|---|---|---|
| 2.1 | WebSocket upgrade: `/ws` echo | ✅ | see `git log -- handler/ws.go` |
| 2.2 | Room: many clients, one broadcast; read and write loops per client | ✅ (actor) | see `git log room-actor` |
| 2.3 | Tick: the room broadcasts state every 50 ms | ⬜ | |
| 2.4 | Shutdown with live connections | ⬜ | |

## 2.1 WebSocket upgrade

HTTP is request → response → done. A WebSocket starts as an HTTP request and is **upgraded** into a long-lived, two-way connection on the same TCP socket:

```
client: GET /ws
        Connection: Upgrade
        Upgrade: websocket
        Sec-WebSocket-Version: 13
        Sec-WebSocket-Key: <base64 of 16 random bytes>
server: 101 Switching Protocols
        ── both sides can now send at any time ──
```

- Go's standard library has no WebSocket server. We use **gorilla/websocket**, the same as tbm-sim.
- `upgrader.Upgrade(c.Writer, c.Request, nil)` returns a `*websocket.Conn`. On failure it has already written an HTTP error, so just log and **return** (`conn` is nil; `defer conn.Close()` would panic).
- The handler runs for the **whole connection**, not one message. So the request log line appears on disconnect, and its `latency` is the connection's lifetime.
- Gin logs `status: 200`, but the client got 101: gorilla takes over the raw connection and writes the 101 itself.
- Echo needs only one loop (`ReadMessage` → `WriteMessage`), because we only write in reply to a read.
- The `Upgrader` zero value rejects browser pages from other origins (403).

Testing:
- Plain `curl` or the address bar → **400**: a normal GET has no upgrade headers.
- curl with the headers shows the 101, but can't speak frames. The key must decode to exactly 16 bytes (`dGhlIHNhbXBsZSBub25jZQ==`), or it's a 400 too. **Check the server log** for the reason.
- Browser: open `http://localhost:8080/livez`, then in the console `ws = new WebSocket("ws://localhost:8080/ws"); ws.onmessage = e => console.log(e.data); ws.send("hi")`.

## Branch `room-actor`: the channel (actor) Room

This branch explores the alternative to `main`'s mutex Room: only `Room.Run` touches the client set, and everyone else talks to it through `join` / `leave` / `broadcast` channels inside one `select`. No mutex, because one goroutine owns the map.

- Go maps crash on concurrent writes (`fatal error: concurrent map writes`): many readLoops and handlers run in their own goroutines, so the map needs either a lock or a single owner.
- `map[*Client]struct{}` is a set: unique keys, and the empty struct takes no memory.
- Risk: a send to a room whose `Run` has stopped blocks forever.
- See the trade-off table on `main` in this file.

### 2.2 on this branch

```go
case msg := <-r.broadcast:
	for c := range r.clients {      // range over a map: each key (a client)
		select {
		case c.send <- msg:         // room in c's buffer: queued
		default:                    // buffer full: drop the slow client, never wait
			delete(r.clients, c)    // deleting during range is allowed
			close(c.send)
		}
	}
```

- `case c := <-r.join:` — `:=` declares `c` right there; its type comes from the channel (`chan *Client` → `*Client`). It exists only inside that case.
- `_, ok := r.clients[c]` — a map lookup can return two values: the stored value and a `bool` "was the key found?". The map stores no bools; Go computes it during the lookup. `_` throws away a slot you don't need.
- Leave checks `ok` before `close`, because broadcast may already have dropped the client, and closing a channel twice panics.
- Only the **sender** closes a channel. The room sends on `c.send`, so the room closes it; that ends writeLoop's `range`.
- `Room.Serve(conn)`: join, `go writeLoop()`, `readLoop()`. Lowercase names (`join`, `readLoop`) are private to the package; capitalised ones (`Serve`, `Run`) are exported. The handler only gets the public door.

Tested: 3 clients, A and B send, C leaves, A sends again → A and B get all three, C the first two. `-race`: 0 data races.

### What differs in 2.3 and 2.4

| | Actor (this branch) | Mutex (`main`) |
|---|---|---|
| 2.3 Tick | one more `case <-ticker.C:` in the same `select` | separate goroutine with a ticker; locks to read state and broadcast (tbm-sim's `broadcastLoop`) |
| 2.4 Shutdown | `Run` needs a stop signal; after it stops, a readLoop's `r.leave <- c` hangs forever | stop the ticker goroutine, close clients under the lock |
