package main

import (
	"fmt"
	"os"
	"strconv"
	"time"
)

// Config holds settings read from environment variables at startup.
type Config struct {
	Port            int
	ShutdownTimeout time.Duration
	DrainDelay      time.Duration
	DBPath          string // SQLite file holding the sessions
}

// LoadConfig reads Config from the environment. Unset variables fall back to
// defaults; set but invalid ones return an error so the app fails at startup.
func LoadConfig() (Config, error) {
	cfg := Config{
		// All these values need to be replaced or checked.
		// ? Should there ever be a default value?
		Port:            8080,
		ShutdownTimeout: 10 * time.Second,
		DrainDelay:      5 * time.Second,
		DBPath:          "gcp-sess.db",
	}

	// LookupEnv, not Getenv: it tells "unset" (use the default) apart from "set to empty" (an error).
	if v, ok := os.LookupEnv("PORT"); ok {
		port, err := strconv.Atoi(v)
		if err != nil || port < 1 || port > 65535 {
			return Config{}, fmt.Errorf("PORT must be a number between 1 and 65535, got %q", v)
		}
		cfg.Port = port
	}

	if v, ok := os.LookupEnv("SHUTDOWN_TIMEOUT"); ok {
		d, err := time.ParseDuration(v)
		if err != nil || d <= 0 {
			return Config{}, fmt.Errorf("SHUTDOWN_TIMEOUT must be a positive duration like 10s, got %q", v)
		}
		cfg.ShutdownTimeout = d
	}

	if v, ok := os.LookupEnv("DRAIN_DELAY"); ok {
		d, err := time.ParseDuration(v)
		if err != nil || d < 5*time.Second {
			return Config{}, fmt.Errorf("DRAIN_DELAY must be at least 5s, got %q", v)
		}
		cfg.DrainDelay = d
	}

	if v, ok := os.LookupEnv("DB_PATH"); ok {
		if v == "" {
			return Config{}, fmt.Errorf("DB_PATH must not be empty")
		}
		cfg.DBPath = v
	}

	return cfg, nil
}
