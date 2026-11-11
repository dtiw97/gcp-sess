// Package machine is the TBM itself: its inputs, its outputs, and how it changes
// over time. It knows nothing about clients, locks, ticks or WebSockets, so it
// can be tested on its own. session imports machine, never the other way round.
package machine

import "time"

// Input is what the operator sets (clients send it).
type Input struct {
	PottyValue float64 `json:"potty_value"` // the speed potentiometer, 0–100
}

// Output is what the machine computes from its inputs over time.
type Output struct {
	Speed    float64 `json:"speed"`    // mm/s, derived from the pot
	Distance float64 `json:"distance"` // mm advanced so far: accumulates every Run
}

// State is the whole machine at one moment: the inputs it was given and what it computed.
type State struct {
	Input  Input  `json:"input"`
	Output Output `json:"output"`
}

// Run advances the machine by dt with the given input and returns the new state.
// Pure: no lock, no clock, no side effects. Same (state, input, dt) → same result.
// ! Value receiver (st State, not *State): st is a copy, so Run can't change the
// caller's state; the caller stores the returned one.
func (st State) Run(in Input, dt time.Duration) State {
	st.Input = in
	st.Output.Speed = in.PottyValue / 10
	// ! dt.Seconds() turns a Duration into float64 seconds (50ms → 0.05).
	// += : distance builds up step after step; that's what makes it stateful.
	st.Output.Distance += st.Output.Speed * dt.Seconds()
	return st
}
