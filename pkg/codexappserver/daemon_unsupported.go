//go:build !darwin && !linux

package codexappserver

import (
	"context"
	"errors"
	"net"
)

func dialExistingDaemon(context.Context, string) (net.Conn, int, error) {
	return nil, 0, errors.New("direct shared Codex daemon connections currently require macOS or Linux")
}
