//go:build !linux && !darwin

package repository

import (
	"fmt"
	"os"
)

func lockLocalFile(string) (*os.File, error) {
	return nil, fmt.Errorf("taskrunner SQLite instance locking is unsupported on this platform")
}
