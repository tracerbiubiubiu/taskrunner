package service

import (
	"log/slog"
	"runtime/debug"
)

// runBackgroundTick isolates one scan, not the lifetime of the loop: after a
// panic the next tick must still execute. Normal shutdown remains ctx-driven.
func runBackgroundTick(logger *slog.Logger, component string, tick func()) {
	defer func() {
		if p := recover(); p != nil {
			logger.Error("background tick panic; next tick will retry",
				slog.String("component", component), slog.Any("panic", p), slog.String("stack", string(debug.Stack())))
		}
	}()
	tick()
}
