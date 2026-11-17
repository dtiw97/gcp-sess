package session

import (
	"context"
	"errors"
	"gcp-sess/repo"
	"log/slog"
	"sync"
	"time"
)

// saveInterval is how often the live session is written to the database.
// A crash loses at most this much progress; a clean shutdown loses nothing (final save).
const saveInterval = 3 * time.Second

var (
	// ErrBusy: the room is running another session that still has clients.
	ErrBusy = errors.New("another session is live")
	// ErrClosed: the server is shutting down and starts no more rooms.
	ErrClosed = errors.New("sessions are shutting down")
)

// Manager owns the server's ONE room (tbm-sim: maxRooms=1) and the database side
// of it: which session the room runs, loading it, saving it every saveInterval.
// The room itself (clients, tick loop) is unchanged: Manager only starts and stops it.
type Manager struct {
	ctx   context.Context // the session context: the room's tick loop stops when it's cancelled
	store *repo.SessionRepo

	// ! mu guards the fields below AND serialises every DB write (periodic save,
	// save on switch, final save), so an old snapshot can never overwrite a newer one.
	// The tick loop never takes it: holding it during disk I/O only delays Room() and Run.
	mu     sync.Mutex
	room   *Room  // the live room; nil until the first connect
	stop   func() // stops room's tick loop and waits until it has returned
	closed bool   // set by Run on shutdown: Room() refuses
}

func NewManager(ctx context.Context, store *repo.SessionRepo) *Manager {
	return &Manager{ctx: ctx, store: store}
}

// Room returns the room running session id, so the handler can upgrade and Serve.
//   - the room already runs id → return it
//   - no room, or the room runs another session with no clients → (save and stop it,) load id, start a new room
//   - the room runs another session with clients → ErrBusy
//
// repo.ErrNotFound if id doesn't exist.
func (m *Manager) Room(ctx context.Context, id string) (*Room, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	if m.closed {
		return nil, ErrClosed
	}
	if m.room != nil && m.room.id == id {
		return m.room, nil
	}
	if m.room != nil && m.room.Len() > 0 {
		return nil, ErrBusy
	}

	snap, err := m.store.Load(ctx, id) // DB read under m.mu: see the note on mu
	if err != nil {
		return nil, err
	}

	if m.room != nil {
		// Switching sessions: nobody is connected to the old one.
		old := m.room
		m.stop()         // its tick loop has returned: the engine won't change any more
		m.save(ctx, old) // so this is its final state
		slog.Info("session stopped", "session", old.id)
		// A client that got old from us but hasn't joined yet finds it closed: Join
		// refuses and it gets 1001; it can reconnect.
	}

	m.start(snap)
	slog.Info("session started", "session", id, "tick", snap.Tick)
	return m.room, nil
}

// start makes a room from a saved snapshot and runs its tick loop. Holds m.mu.
func (m *Manager) start(snap repo.Snapshot) {
	r := NewRoom(snap.ID, RestoreEngine(snap.Tick, snap.Input, snap.State))

	// A child of m.ctx: shutdown stops it, and so can a session switch (cancel).
	roomCtx, cancel := context.WithCancel(m.ctx)
	done := make(chan struct{}) // closed when Run returns
	go func() {
		defer close(done)
		r.Run(roomCtx) // ticks until cancelled, then closeAll (1001)
	}()

	m.room = r
	m.stop = func() {
		cancel()
		<-done // ! wait: until Run returns, the engine may still step
	}
}

// Run is the save loop: every saveInterval it writes the live session to the DB.
// On shutdown it stops the room (clients get 1001), then saves one last time.
func (m *Manager) Run() {
	ticker := time.NewTicker(saveInterval)
	defer ticker.Stop()

	for {
		select {
		case <-m.ctx.Done():
			m.mu.Lock()
			defer m.mu.Unlock()
			m.closed = true // Room() refuses from now on
			if m.room == nil {
				return // nobody ever connected
			}
			m.stop() // the room's Run has closed every client and returned

			// m.ctx is cancelled, so the final save needs a fresh context.
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			m.save(ctx, m.room)
			slog.Info("session saved on shutdown", "session", m.room.id)
			return

		case <-ticker.C:
			m.mu.Lock()
			if m.room != nil {
				m.save(m.ctx, m.room)
			}
			m.mu.Unlock()
		}
	}
}

// save writes one room's engine to the DB. Callers hold m.mu.
func (m *Manager) save(ctx context.Context, r *Room) {
	tick, in, st := r.engine.Snapshot() // engine lock on/off: a consistent copy
	err := m.store.Save(ctx, repo.Snapshot{ID: r.id, Tick: tick, Input: in, State: st})
	if err != nil {
		slog.Error("save session failed", "session", r.id, "err", err)
	}
}
