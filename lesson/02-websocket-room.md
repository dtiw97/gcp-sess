# Module 2: WebSocket room + tick (mini tbm-sim)

Goal: turn the service into a **session server**: clients stay connected and share live state. This is what Agones will later manage, one session per pod.

## Lessons

| # | Lesson | Status | Commit |
|---|---|---|---|
| 2.1 | WebSocket upgrade: `/ws` echo | ✅ | see `git log -- handler/ws.go` |
| 2.2 | Room: many clients, one broadcast; read and write loops per client | ✅ (mutex) | see `git log -- session/room.go` |
| 2.3 | Tick: a machine model, an engine that runs it, a tick loop that broadcasts | ✅ (mutex) | see `git log -- machine/` |
| 2.4 | Shutdown with live connections | ✅ (mutex) | see `git log -- session/client.go` |

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

### 2.3 on `main`: machine, engine, tick loop

Authoritative server: clients send **inputs**, the server computes, and the tick is the only broadcast.

```
IN:  client ─▶ readLoop: json.Unmarshal → engine.Apply(input)      (stores it; nothing computed)
     tick loop, every 50 ms (room.Run, started by wg.Go in main):
       state := engine.Step(50ms)    engine lock on/off, returns a copy
       data  := json.Marshal(state)  no lock
       room.Broadcast(data)          room lock on/off
OUT: client ◀─ writeLoop ◀─ c.send
```

Three layers, three homes:

| Layer | Where | Knows about |
|---|---|---|
| the machine (model) | `machine/state.go`, package `machine` | input + time → output. Pure: `func (st State) Run(in Input, dt time.Duration) State` |
| runs the machine | `session/engine.go`, `Engine` | the one live State, the latest input, the tick count, its mutex |
| the clients | `session/room.go`, `Room` | the client set, `Broadcast`, the tick loop `Run` |

- `session` imports `machine`, never the reverse; Go forbids import cycles, so the compiler enforces it. `machine` is testable with no server: `State{}.Run(Input{PottyValue: 200}, 50*time.Millisecond)`.
- **Time is a parameter.** `dt time.Duration`; `dt.Seconds()` → float64 (50 ms → 0.05). An `int` of seconds turns 50 ms into 0. `+=` makes it stateful (distance accumulates).
- **Value receiver = pure.** `st` is a copy; `Run` returns the new State, the engine stores it.
- **Input persists**, like a real pot: no new input → the machine keeps running on the last one. Last write in a tick wins; it takes effect on the next tick, for everyone.
- The tick count lives in the Engine (bookkeeping), not in the State. Clients don't need it: TCP keeps order, and a slow client is disconnected, never skipped, so the latest full state is enough. (Sending diffs, or skipping states, would need a sequence number.)

**Why `Run` needs its own goroutine.** Defining a method doesn't start it. `Run` loops until the session ends, so called directly, main would stop there and never serve (in 2.3 it was `go room.Run()`; 2.4 makes it `wg.Go(...)` so main can wait for it). And ticking needs a goroutine of its own: Join/Leave/Broadcast borrow a client's goroutine, but the machine must advance when nobody sends anything. A ticker holds it to real time; a bare `for {}` would run millions of steps a second.

**Two mutexes, never held together.** `Room.mu` guards the client set; `Engine.mu` guards input/state/tick. The engine is shared by every readLoop (`Apply` writes `input`) and the tick loop (`Step` reads `input`, writes `state`). writeLoops never touch it. The tick loop releases the engine's lock before taking the room's, so no lock-ordering deadlock.

**Why `Step` returns a copy.** The tick loop encodes `state` after `Step` has unlocked. A pointer to the engine's state would be read unlocked while the next `Step` writes it: a data race, and maybe a half-old, half-new frame.

**`json.Unmarshal(data, &input)`**: bytes → struct; `&` so it fills *our* variable. Bad JSON → log, `continue`, keep the connection and the last good input.

**Bug found:** readLoop still called `c.room.Broadcast(data)` from 2.2, so every input was relayed raw alongside the states (29 messages, 1 not a state). Clients expecting only states would break, and they'd see inputs before the server applied them. In 2.3 the tick loop is the only sender.

**Dummy frontend:** `web/index.html` (React from a CDN, no build), served at `http://localhost:8080/ui` by `router.StaticFile`. It must be served by this server: the Upgrader rejects other origins, so `file://` or a dev server on another port gets 403. Slider → `{"potty_value": N}`; cards show speed, distance, the server's input, states/s (~20).

**Deferred requirement (tbm-sim):** many commands in one envelope `{command: {group, action, args}}`; group `session` (start/pause/reset) → run state; group `tbm` → a command registry mutating the persistent control panel; gates before a command lands (paused → drop tbm commands). Paused = clock frozen, frames still broadcast.

Tested: pot 100 for ~1.2 s → `{"input":{"potty_value":100},"output":{"speed":10,"distance":12}}`; 29 messages, all states; "not json" logged as bad input, connection kept; `-race`: 0.

### 2.4 on `main`: shutdown with live connections

Goal: on SIGTERM every client gets a clean **1001**, not a 1006, and nothing leaks.

```
SIGTERM → ready=false → drain (session still ticking) → server.Shutdown (no new clients)
        → stopSess() → Run sees ctx.Done → closeAll → return → wg.Wait() returns → exit
```

- **`sessCtx` from `context.Background()`, not `appCtx`.** `appCtx` is cancelled by SIGTERM; the session must keep ticking through the drain. Only main cancels it, last.
- **`for` + `select`** in `Run`: `for range ticker.C` can wait on one channel only; `select` waits on `ctx.Done()` and `ticker.C`, whichever first. `return`, not `break` (break only leaves the select).
- **`closeAll`, two phases:** under the lock, delete each client and `close(c.send)`, collecting them in a slice; unlock; then `<-c.done` for each. Waiting under the lock would block every readLoop's `Leave`. A later `Leave` finds `ok == false`: no double close.
- **Why `done`.** The WaitGroup counts only `Run`. `close(c.send)` only *asks* writeLoop to stop; without waiting, main exits and kills the writeLoops before their close frame (tested: 8 of 15 clients got 1006). The chain: `wg.Wait` → `Run` → `closeAll` → `<-c.done` → writeLoop's `defer close(c.done)`. `done` carries no data: open = still writing, closed = finished (flushed the queue, sent 1001, closed conn). Defers run in reverse, so `close(c.done)` is registered first to run last.
- **1001 vs 1005 vs 1006.** `FormatCloseMessage(CloseGoingAway, "session closed")` → 1001. An empty close body → 1005 (no code). No close frame at all → 1006 (abnormal).
- **Both Shutdown paths stop the session.** A Shutdown timeout cut off slow HTTP requests; the WebSocket clients still get their goodbye. No early `return`.
- **Write deadline.** `SetWriteDeadline(time.Now().Add(writeWait))` before every write: a moment on the clock, so reset each time. Healthy writes take microseconds (they only fill the OS socket buffer); a write blocks only when the peer stopped acknowledging. Without it a dead client's writeLoop could block ~15 min, and so would `closeAll`. `Broadcast` decides "you're out"; the deadline makes sure that goroutine actually exits. Worst-case shutdown: drain 5 s + Shutdown 10 s + writeWait 5 s < Kubernetes' 30 s grace.
- Not for now: client reconnects. A **reconnect storm** comes from the server going away (all clients at once); clients fix it with backoff + jitter. Modules 11–13.

Tested (`-race`, 3 clients, SIGTERM): 9/9 got `1001 clean=true` over 3 runs, ~116 states each (kept ticking through the 5 s drain); with Shutdown forced to time out: 3/3 1001. Not tested: a real stalled network.

### Mutex vs actor, after 2.4

| | Mutex (`main`) | Actor (`room-actor`) |
|---|---|---|
| Shutdown cost | one `closeAll`; nothing else changed | `Room.done` + a `select` on every send to the room |
| Hazard | wait under the lock; forget a lock (`-race` finds it) | send to a stopped actor hangs forever (found only by a goroutine dump) |
| Order of events | whoever locks first | one queue: the Run loop sees everything in order |
| Goroutines | tick loop only | Room loop (+ Sim loop) |
| Reads its code as | methods on shared data | messages to an owner |

Both needed the same client-side pieces: `done`, 1001, the WaitGroup chain. The difference is the room: a stopped mutex Room is just data (calling it later is safe); a stopped actor is a goroutine nobody listens to.

### tbm-sim design note (for later)

tbm-sim uses goroutine loops per actor (`oklog/run`) plus a session mutex around the tick. A direction worth weighing: the session loop is the **sole owner** of sim state, commands arrive on a channel and are applied at tick boundaries, and each tick **publishes an immutable snapshot** (`atomic.Pointer`) that broadcast, HTTP and overlays read without locking. Gains: deterministic replays, no lock on the 50 ms hot path, no lock-ordering deadlocks across actors. Costs: snapshots must be real copies; commands wait up to one tick. (Based on the docs, not the code.)
