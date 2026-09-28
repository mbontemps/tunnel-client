//go:build darwin

package codexappserver

import (
	"errors"
	"net"
	"os"

	"golang.org/x/sys/unix"
)

func daemonPeer(conn *net.UnixConn) (int, error) {
	raw, err := conn.SyscallConn()
	if err != nil {
		return 0, err
	}
	var pid int
	var peerErr error
	err = raw.Control(func(fd uintptr) {
		cred, err := unix.GetsockoptXucred(int(fd), unix.SOL_LOCAL, unix.LOCAL_PEERCRED)
		if err != nil {
			peerErr = err
			return
		}
		if int(cred.Uid) != os.Geteuid() {
			peerErr = errors.New("daemon peer belongs to another user")
			return
		}
		pid, peerErr = unix.GetsockoptInt(int(fd), unix.SOL_LOCAL, unix.LOCAL_PEERPID)
	})
	if err != nil {
		return 0, err
	}
	return pid, peerErr
}
