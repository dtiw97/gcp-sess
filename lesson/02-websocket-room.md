# Module 2: WebSocket room + tick (mini tbm-sim)

Goal: turn the service into a **session server**: clients stay connected and share live state. This is what Agones will later manage, one session per pod.

## Lessons

| # | Lesson | Status | Commit |
|---|---|---|---|
| 2.1 | WebSocket upgrade: `/ws` echo | ✅ | see `git log -- handler/ws.go` |
| 2.2 | Room: many clients, one broadcast; read and write loops per client | ⬜ | |
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
