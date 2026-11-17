// Package repo is the storage layer: it saves and loads sessions in SQLite.
// It knows rows and SQL, nothing about rooms, clients or WebSockets.
package repo

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"gcp-sess/machine"
	"time"

	// ! A blank import: we never call the package by name. Importing it runs its
	// init(), which registers the "sqlite" driver with database/sql.
	// modernc.org/sqlite is pure Go (no cgo), so the binary stays static for Module 3.
	_ "modernc.org/sqlite"
)

// ErrNotFound means no session has that id.
var ErrNotFound = errors.New("session not found")

// Session is one row: the session id plus the latest saved snapshot of its machine.
type Session struct {
	ID        string        `json:"id"`
	Tick      int           `json:"tick"`
	State     machine.State `json:"state"`
	CreatedAt time.Time     `json:"created_at"`
	UpdatedAt time.Time     `json:"updated_at"`
}

// Snapshot is what the session saves every few seconds: the engine's live values.
type Snapshot struct {
	ID    string
	Tick  int
	Input machine.Input
	State machine.State
}

// SessionRepo stores sessions in one SQLite file.
type SessionRepo struct {
	db *sql.DB
}

// Open opens (or creates) the SQLite file at path and makes sure the table exists.
func Open(path string) (*SessionRepo, error) {
	// WAL: readers don't wait for the writer. busy_timeout: wait up to 5 s for the
	// file lock instead of failing at once with SQLITE_BUSY.
	dsn := fmt.Sprintf("file:%s?_pragma=journal_mode(WAL)&_pragma=busy_timeout(5000)", path)
	db, err := sql.Open("sqlite", dsn)
	if err != nil {
		return nil, err
	}
	// SQLite allows one writer at a time; one connection makes database/sql queue
	// our writes instead of racing for the file lock.
	db.SetMaxOpenConns(1)

	// input and state are JSON text: the machine's fields can change without a migration.
	const schema = `
CREATE TABLE IF NOT EXISTS sessions (
	id         TEXT PRIMARY KEY,
	tick       INTEGER NOT NULL DEFAULT 0,
	input      TEXT NOT NULL DEFAULT '{}',
	state      TEXT NOT NULL DEFAULT '{}',
	created_at INTEGER NOT NULL,
	updated_at INTEGER NOT NULL
)`
	if _, err := db.Exec(schema); err != nil {
		db.Close()
		return nil, fmt.Errorf("create schema: %w", err)
	}
	return &SessionRepo{db: db}, nil
}

func (r *SessionRepo) Close() error { return r.db.Close() }

// Create inserts a new, empty session.
func (r *SessionRepo) Create(ctx context.Context, id string) (Session, error) {
	now := time.Now().UTC()
	_, err := r.db.ExecContext(ctx,
		`INSERT INTO sessions (id, created_at, updated_at) VALUES (?, ?, ?)`,
		id, now.UnixMilli(), now.UnixMilli())
	if err != nil {
		return Session{}, err
	}
	return Session{ID: id, CreatedAt: now, UpdatedAt: now}, nil
}

// Get returns one session, or ErrNotFound.
func (r *SessionRepo) Get(ctx context.Context, id string) (Session, error) {
	row := r.db.QueryRowContext(ctx,
		`SELECT id, tick, state, created_at, updated_at FROM sessions WHERE id = ?`, id)
	s, err := scanSession(row)
	if errors.Is(err, sql.ErrNoRows) {
		return Session{}, ErrNotFound
	}
	return s, err
}

// Load returns what a room needs to resume: the saved tick, input and state.
func (r *SessionRepo) Load(ctx context.Context, id string) (Snapshot, error) {
	var (
		snap         Snapshot
		input, state string
	)
	err := r.db.QueryRowContext(ctx,
		`SELECT id, tick, input, state FROM sessions WHERE id = ?`, id).
		Scan(&snap.ID, &snap.Tick, &input, &state)
	if errors.Is(err, sql.ErrNoRows) {
		return Snapshot{}, ErrNotFound
	}
	if err != nil {
		return Snapshot{}, err
	}
	if err := json.Unmarshal([]byte(input), &snap.Input); err != nil {
		return Snapshot{}, fmt.Errorf("decode input: %w", err)
	}
	if err := json.Unmarshal([]byte(state), &snap.State); err != nil {
		return Snapshot{}, fmt.Errorf("decode state: %w", err)
	}
	return snap, nil
}

// List returns every session, newest first.
func (r *SessionRepo) List(ctx context.Context) ([]Session, error) {
	rows, err := r.db.QueryContext(ctx,
		`SELECT id, tick, state, created_at, updated_at FROM sessions ORDER BY created_at DESC`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	out := []Session{} // not nil: encodes as [] instead of null
	for rows.Next() {
		s, err := scanSession(rows)
		if err != nil {
			return nil, err
		}
		out = append(out, s)
	}
	return out, rows.Err()
}

// Save writes one snapshot over the session's row.
func (r *SessionRepo) Save(ctx context.Context, s Snapshot) error {
	input, err := json.Marshal(s.Input)
	if err != nil {
		return err
	}
	state, err := json.Marshal(s.State)
	if err != nil {
		return err
	}
	_, err = r.db.ExecContext(ctx,
		`UPDATE sessions SET tick = ?, input = ?, state = ?, updated_at = ? WHERE id = ?`,
		s.Tick, string(input), string(state), time.Now().UTC().UnixMilli(), s.ID)
	return err
}

// scanner is what *sql.Row and *sql.Rows have in common, so one helper reads both.
type scanner interface {
	Scan(dest ...any) error
}

func scanSession(sc scanner) (Session, error) {
	var (
		s                Session
		state            string
		created, updated int64
	)
	if err := sc.Scan(&s.ID, &s.Tick, &state, &created, &updated); err != nil {
		return Session{}, err
	}
	if err := json.Unmarshal([]byte(state), &s.State); err != nil {
		return Session{}, fmt.Errorf("decode state: %w", err)
	}
	s.CreatedAt = time.UnixMilli(created).UTC()
	s.UpdatedAt = time.UnixMilli(updated).UTC()
	return s, nil
}
