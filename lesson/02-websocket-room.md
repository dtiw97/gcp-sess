# Module 2: WebSocket room + tick (mini tbm-sim)

Goal: turn the service into a **session server**: clients stay connected and share live state. This is what Agones will later manage, one session per pod.

## Lessons

| # | Lesson | Status | Commit |
|---|---|---|---|
| 2.1 | WebSocket upgrade: `/ws` echo | ✅ | see `git log -- handler/ws.go` |
| 2.2 | Room: many clients, one broadcast; read and write loops per client | ✅ (mutex) | see `git log -- session/room.go` |
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
- Browser: open `http://localhost:8080/x` (a plain-text 404; Firefox's JSON viewer on `/livez` blocks WebSockets with its `default-src 'none'` policy), then in the console `ws = new WebSocket("ws://localhost:8080/ws"); ws.onmessage = e => console.log(e.data); ws.send("hi")`.

## 2.2 Room: two designs

Each client has two loops: **readLoop** (browser → room) and **writeLoop** (`send` channel → browser). writeLoop is the only writer to the conn (gorilla allows one), and `WriteMessage` can wait on a slow network, so each client waits in its own goroutine and never holds up the room.

The client set is shared: many goroutines (handlers, readLoops) touch it at once, and Go maps crash on concurrent writes. Two ways to protect it:

| | Mutex (`main`, tbm-sim style) | Channels / actor (branch `room-actor`) |
|---|---|---|
| Map access | any method, while holding the lock | only `Room.Run` |
| API | `room.Join(c)`, `Leave`, `Broadcast` | sends on `join` / `leave` / `broadcast` |
| Typical bug | forgot to lock; slow work under the lock; two locks taken in the wrong order → deadlock | send to a stopped room blocks forever; one huge `select` |
| Fits | guarding state | passing work or ownership, coordination |

Go's rule of thumb: **mutex to guard state, channels to pass work or coordinate.** At 9 clients and 20 ticks/s neither is a performance issue; tbm-sim scales by adding pods, not by making a room faster.

### 2.2 on `main`: the mutex Room

```
handler: Upgrade → room.Serve(conn)
Serve:   NewClient(r, conn) → r.Join(c) → go c.writeLoop() → c.readLoop()
readLoop: each message → r.Broadcast(data);  on exit → r.Leave(c)
```

**Mutex from scratch.** A mutex is one key: `Lock()` takes it, or **waits** until it's free; `Unlock()` gives it back. Code between them (the critical section) runs one goroutine at a time.

- The mutex doesn't know what it protects. "Every access to `clients` holds `mu`" is a promise kept in code; `-race` catches a forgotten lock.
- `r.mu.Lock(); defer r.mu.Unlock()`: you can't forget to unlock, even with an early return. Never unlocked = every later `Lock()` waits forever.
- **Keep it short.** Everyone waits while you hold it. `Broadcast` only queues (`select`/`default` on `c.send`); the slow network writes happen in each writeLoop, outside the lock.
- **Not re-entrant.** `Broadcast` drops slow clients inline instead of calling `Leave`, because `Leave` locks too: a goroutine waiting for a key it holds waits forever.
- **Don't copy it.** Pointer receivers (`func (r *Room)`). The zero value is an unlocked mutex: no `sync.Mutex{}` needed.
- No Room goroutine: unlike the actor, a mutex Room is just data plus a lock. `main` does `room := session.NewRoom()` and nothing else.

**Same order for everyone.** One `Broadcast` queues its message to *every* client before the next can lock, so all clients see messages in the same order. Which of two simultaneous messages goes first is up to who locks first.

**The `ok` check, which end it's on.** `Leave`'s `c` comes from outside (readLoop) and may already have been dropped by `Broadcast`; closing `c.send` twice panics, so check first. Check and close happen under the lock, so nobody closes in between. `Broadcast`'s `c` comes from the map itself while holding the lock: no check needed.

**A `select` case is the operation.** `case c.send <- msg:` *does* the send. A second `c.send <- msg` in its body sends twice, and could block while holding the lock, freezing the room.

**Go syntax met here:**

- `map[*Client]struct{}` is a set. `struct{}` is the type (0 bytes), `struct{}{}` its only value. Add `m[c] = struct{}{}`; check `_, ok := m[c]`; remove `delete(m, c)`.
- No loop to find one key: `m[c]` looks it up directly. `for c := range m` gives keys; `for k, v := range m` keys and values; `for _, v := range m` values only (here useless `struct{}{}`, and it shadows `c`).
- `&Room{}` makes a **new** room with a **nil map** (writes panic). Use `NewRoom()`, once, in `main`, and pass the pointer around: every client must share the same Room.
- Lowercase fields (`mu`, `clients`) are private to package `session`: `api/handler` can't build a Room itself, which is why `NewRoom` exists.

**Ownership.** `main` creates the Room and passes it to `NewWS(room)`, like `health`. The handler only upgrades and calls `room.Serve(conn)`, the one public door; the session runs the connection, so the session closes it (no `defer conn.Close()` in the handler: by the time `Serve` returns, readLoop has closed it). The echo loop's parts moved: reading and "ws disconnected" → readLoop; writing and "ws write failed" → writeLoop; "ws connected" stays in the handler (it has the Gin context's `ClientIP()`). `msgType` is dropped: the server decides, always `TextMessage`.

**Timing.** Each client has 2 goroutines (cheap). Different send times are fine: the lock orders them. Different network speeds are fine: own writeLoop, drop when the buffer is full. Who decides the *game* state when inputs arrive at different times is 2.3's tick.

Tested: 3 clients, A sends `a1`, B `b1`, C leaves, A `a2` → A and B `[a1 b1 a2]`, C `[a1 b1]`; `-race`: 0 data races.

### tbm-sim design note (for later)

tbm-sim uses goroutine loops per actor (`oklog/run`) plus a session mutex around the tick. A direction worth weighing: the session loop is the **sole owner** of sim state, commands arrive on a channel and are applied at tick boundaries, and each tick **publishes an immutable snapshot** (`atomic.Pointer`) that broadcast, HTTP and overlays read without locking. Gains: deterministic replays, no lock on the 50 ms hot path, no lock-ordering deadlocks across actors. Costs: snapshots must be real copies; commands wait up to one tick. (Based on the docs, not the code.)
