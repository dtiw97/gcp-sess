# Module 2: WebSocket room + tick (mini tbm-sim)

Goal: turn the service into a **session server**: clients stay connected and share live state. This is what Agones will later manage, one session per pod.

## Lessons

| # | Lesson | Status | Commit |
|---|---|---|---|
| 2.1 | WebSocket upgrade: `/ws` echo | ✅ | see `git log -- handler/ws.go` |
| 2.2 | Room: many clients, one broadcast; read and write loops per client | ✅ (actor) | see `git log room-actor` |
| 2.3 | Tick: a Sim actor advances state every 50 ms; the Room delivers it | ✅ (actor) | see `git log room-actor` |
| 2.4 | Shutdown with live connections: clients get 1001, actors stop, nothing leaks | ✅ (actor) | see `git log room-actor` |

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

### 2.3 on this branch: Sim and Room, two actors

Authoritative server: the server owns the state and advances it on a clock; clients send **commands** and display what comes back. With a tick, the tick **is** the broadcast: the old "relay to everyone" path is gone.

```
client ─readLoop─▶ Room.commands ─▶ Sim.commands   (IN: change the state)
                                     Sim: tick every 50 ms
client ◀writeLoop─ Room.sendAll  ◀─ Sim.output     (OUT: the computed state)
```

- **Sim** (`sim.go`) owns `State` and the ticker; knows nothing about clients. Mirrors tbm-sim's EPBSession.
- **Room** (`room.go`) owns the client set; forwards commands, encodes each state as JSON once, fans it out, drops slow clients. Mirrors tbm-sim's Room.
- Each actor owns its data; **channels are the only way data crosses**, so no locks.
- `time.NewTicker(d)`: `ticker.C` is a channel that gets a value every `d`. `defer ticker.Stop()`.
- The sim sends to `output` with `select`/`default`: it **never waits** on the Room. If the Room hasn't taken the last state, this one is skipped; a fresher one comes in 50 ms.
- `output` has a buffer of 1 (newest state); `commands` a buffer of 64 (the Room hands over without waiting).
- Sending a struct value sends a **copy**: the Room can't modify the sim's state.
- Struct tags `` `json:"tick"` `` name the JSON fields. `json.Marshal` → `(data, err)`. `continue` skips to the next loop round.
- Naming: `state` is the data, `output` the pipe. Avoid names that differ by one letter (`state`/`states`).
- Trade-off to tune: 20 states/s with a send buffer of 16 drops a client that stalls for ~0.8 s.

Tested: B gets 20 states/s, sees A's command on the next ticks, 0 skipped ticks, 0 races.

### 2.4 on this branch: shutdown with live connections

Before: Ctrl+C → the client got states through the drain, then **1006** (abnormal). `server.Shutdown` returned in ~90µs because it doesn't track hijacked WebSocket conns, and Sim/Room had no way to stop.

After:

```
SIGTERM → readiness 503 → drain → server.Shutdown (no new connections)
  → stopSession() → Room: drop every client (1001 close frame), wait for their writeLoops
                  → Sim: last chance to save state
  → wg.Wait() → exit
```

**Contexts.** `context.Background()` is an empty root, never cancelled, knows nothing. The wrappers attach the "who cancels me" rule:

```
context.Background()
 ├── appCtx       (NotifyContext)  ← the OS signal
 ├── sessCtx      (WithCancel)     ← main, via stopSession()
 └── shutDownCtx  (WithTimeout)    ← a timer
```

- Cancelling a parent cancels its children, never the reverse. `WithCancel(appCtx)` would freeze the session at SIGTERM, during the drain.
- `ctx.Done()` is a channel that gets **closed** on cancel. A closed channel never blocks, so every waiter wakes at once (a broadcast).
- `case <-ctx.Done(): ... return`. `return` ends `Run`. **`break` inside a `select` only leaves the `select`**, so the `for` would go round again.

**main must wait.** When `main` returns, every goroutine dies. `sync.WaitGroup` is a counter: `wg.Go(f)` = Add(1) + `go f` + Done() when `f` returns; `wg.Wait()` blocks until 0. `func() { sim.Run(sessCtx) }` is an anonymous function, because `wg.Go` wants a `func()` with no arguments.

**Waiting for the Sim and Room isn't enough.** The close frame is written by each client's writeLoop, a different goroutine. Experiment: waiting only for Sim and Room gave 1001 **once in 3 runs** (a race). Each Client got a `done` channel, closed by writeLoop's first defer (defers run in reverse, so it's closed last, after `conn.Close()`). The Room drops **everyone first**, then waits for each `done`: all close frames go out in parallel, so the total is about the slowest client. After: 1001 in 5 out of 5.

**Sending to a stopped actor hangs.** After `Run` returns, readLoop's `c.room.leave <- c` has no receiver and waits forever (a leaked goroutine; invisible from outside, found with a goroutine dump). Fix: `Room.done`, closed by `defer close(r.done)` in `Run`, and **every send to the Room** (`join`, `leave`, `commands`) becomes:

```go
select {
case c.room.leave <- c: // room running
case <-c.room.done:     // room stopped: give up
}
```

The `ok` check in Run's `leave` case is the other end of that channel and solves a different problem (closing `c.send` twice).

- A `chan` field's zero value is `nil`. `close(nil)` panics; `<-nil` blocks forever. Always `make` it.
- Close code: an empty close body shows as **1005**. `websocket.FormatCloseMessage(websocket.CloseGoingAway, "server shutting down")` → **1001**. (Slow clients dropped by `sendAll` get it too: fine for now.)
- "sim stopped" and "room stopped" can log in either order: both wake on the same `sessCtx`.

Tested (`-race`, 5 runs): server logs `server shut down complete → room stopped / sim stopped → session stopped`; the client gets `1001 "server shutting down" wasClean=true`; goroutine dump: 0 stuck readLoops (before 2b: 1).
