package session

import (
	"gcp-sess/machine"
	"sync"
	"time"
)

// tickInterval is how often the tick loop steps the engine (tbm-sim: 50 ms).
// ! A const value of type time.Duration. time.Second / time.Millisecond are
// values too, not types: you multiply them, you don't declare fields with them.
const tickInterval = 50 * time.Millisecond

// Engine runs the machine for this session: it holds the one live machine.State,
// the latest input from clients, and the tick count. The machine computes; the
// engine stores, counts and guards.
// readLoops (Apply) and the tick loop (Step) touch it at the same time, so every
// access holds mu.
type Engine struct {
	mu    sync.Mutex    // guards everything below
	tick  int           // how many steps have run (bookkeeping, not sent to clients)
	input machine.Input // latest input from any client; stays until changed, like a real pot
	state machine.State // the live machine
}

func NewEngine() *Engine {
	return &Engine{} // zero values are ready: unlocked mutex, tick 0, machine at rest
}

// RestoreEngine resumes a saved session: same tick count, same pot position, same machine.
func RestoreEngine(tick int, in machine.Input, st machine.State) *Engine {
	return &Engine{tick: tick, input: in, state: st}
}

// Apply records a client's input. Called by readLoop.
// It doesn't compute anything: the next Step uses it, so every client sees the
// change take effect on the same step. Last one in a tick wins.
func (e *Engine) Apply(in machine.Input) {
	e.mu.Lock()
	defer e.mu.Unlock()

	e.input = in
}

// Step advances the machine by dt and returns a copy of the new state for
// broadcasting. Called by the tick loop with tickInterval.
func (e *Engine) Step(dt time.Duration) machine.State {
	e.mu.Lock()
	defer e.mu.Unlock()

	e.state = e.state.Run(e.input, dt) // the machine computes; the engine stores
	e.tick++

	// ! machine.State is a plain struct: this returns a copy, safe to use after unlock.
	return e.state
}

// Snapshot copies the engine's values for saving. Called by the Manager's save loop.
// ! Three plain values are returned: copies, safe to use after unlock.
func (e *Engine) Snapshot() (tick int, in machine.Input, st machine.State) {
	e.mu.Lock()
	defer e.mu.Unlock()

	return e.tick, e.input, e.state
}
