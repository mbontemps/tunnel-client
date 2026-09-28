//go:build darwin || linux

package codexappserver

import (
	"context"
	"errors"
	"fmt"
	"net"
	"os"
	"path/filepath"
	"syscall"
)

func dialExistingDaemon(ctx context.Context, socket string) (net.Conn, int, error) {
	if !filepath.IsAbs(socket) {
		return nil, 0, errors.New("daemon socket must be an absolute path")
	}
	physical, err := filepath.EvalSymlinks(socket)
	if err != nil {
		return nil, 0, err
	}
	for _, path := range []string{socket, filepath.Dir(socket), physical, filepath.Dir(physical)} {
		info, err := os.Stat(path)
		if err != nil {
			return nil, 0, err
		}
		owner, ok := info.Sys().(*syscall.Stat_t)
		if !ok || int(owner.Uid) != os.Geteuid() || info.Mode().Perm()&0022 != 0 {
			return nil, 0, fmt.Errorf("daemon socket/path must be owned by the current user and not group/world-writable: %s", path)
		}
	}
	info, err := os.Stat(physical)
	if err != nil {
		return nil, 0, err
	}
	if info.Mode()&os.ModeSocket == 0 {
		return nil, 0, errors.New("daemon endpoint is not a Unix socket")
	}
	var dialer net.Dialer
	conn, err := dialer.DialContext(ctx, "unix", physical)
	if err != nil {
		return nil, 0, err
	}
	pid, err := daemonPeer(conn.(*net.UnixConn))
	if err != nil {
		_ = conn.Close()
		return nil, 0, err
	}
	return conn, pid, nil
}
