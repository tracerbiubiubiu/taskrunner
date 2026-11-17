//go:build linux || darwin

package repository

import (
	"errors"
	"fmt"
	"os"
	"syscall"
)

func lockLocalFile(path string) (*os.File, error) {
	f, err := os.OpenFile(path, os.O_CREATE|os.O_RDWR, 0600)
	if err != nil {
		return nil, fmt.Errorf("instance lock file: %w", err)
	}
	if err := syscall.Flock(int(f.Fd()), syscall.LOCK_EX|syscall.LOCK_NB); err != nil {
		f.Close()
		if errors.Is(err, syscall.EWOULDBLOCK) || errors.Is(err, syscall.EAGAIN) {
			return nil, ErrInstanceBusy
		}
		return nil, fmt.Errorf("instance file lock: %w", err)
	}
	return f, nil
}
