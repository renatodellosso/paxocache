package tests

import (
	"log/slog"
	"os"
)

func SilentLogger() *slog.Logger {
	options := slog.HandlerOptions{
		Level: slog.LevelError + 1,
	}
	
	return slog.New(slog.NewJSONHandler(os.Stdout, &options))
}