package session

import (
	"context"
	"log/slog"
	"time"
)

// tickInterval is how often the sim advances its state (tbm-sim: 50 ms).
const tickInterval = 50 * time.Millisecond

// State is what every client sees. The sim owns it; clients only send commands.
// ! The `json:"..."` tags name each field in the JSON output (otherwise "Tick", "Last").
type State struct {
	Tick int    `json:"tick"`
	Last string `json:"last"` // the last command received
}

// Sim runs the simulation on its own clock, separate from the Room.
// It knows nothing about clients or WebSockets: commands come in, states go out.
// Only the Run goroutine touches state, so it needs no mutex.
type Sim struct {
	state State

	commands chan []byte // IN: from the Room; applied at the next tick
	output   chan State  // OUT: to the Room, one State per tick
}

func NewSim() *Sim {
	return &Sim{
		// Buffered so the Room can hand over a command without waiting for the sim.
		commands: make(chan []byte, 64),
		// Buffer of 1: holds the newest state until the Room takes it.
		output: make(chan State, 1),
	}
}

// Run is the sim's own goroutine: main starts it once with `wg.Go(func() { sim.Run(sessCtx) })`.
// It returns when ctx is cancelled.
func (s *Sim) Run(ctx context.Context) {
	// ! ticker.C is a channel; Go puts a value in it every tickInterval.
	ticker := time.NewTicker(tickInterval)
	defer ticker.Stop() // turn the timer off when Run returns

	for {
		select {
		case <-ctx.Done():
			// ! Done() is a channel that gets closed on cancel; a closed channel never blocks.
			// Last chance to save state (tbm-sim would persist here).
			slog.Info("sim stopped", "tick", s.state.Tick)
			return // ! ends Run itself, not just the select; defer then stops the ticker

		case cmd := <-s.commands:
			// Change the state only. It goes out with the next tick.
			s.state.Last = string(cmd) // ! []byte → string conversion

		case <-ticker.C:
			s.state.Tick++ // advance the sim one step

			// Hand the new state to the Room, but never wait for it: the sim must keep
			// ticking even if the Room is busy. If the Room hasn't taken the previous
			// state yet, skip this one; a fresher one comes in 50 ms.
			// ! State is a plain struct, so this sends a copy: the Room can't change ours.
			select {
			case s.output <- s.state:
			default:
			}
		}
	}
}
