package tests

import (
	"log/slog"
	"os"
)

type Dummy struct {
	// Must be uppercase to be serialized into JSON
	X int
}

func SilentLogger() *slog.Logger {
	options := slog.HandlerOptions{
		Level: slog.LevelError + 1,
	}
	
	return slog.New(slog.NewJSONHandler(os.Stdout, &options))
}