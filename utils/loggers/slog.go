package loggers

import (
	"log/slog"
	"os"
)

func Setup() {
	jsonSlog := slog.NewJSONHandler(os.Stdout, nil)
	newSlog := slog.New(jsonSlog)
	slog.SetDefault(newSlog)
}
