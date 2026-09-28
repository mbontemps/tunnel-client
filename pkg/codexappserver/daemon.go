package codexappserver

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"sync"
	"time"

	"github.com/gorilla/websocket"
)

// Each bridge owns a distinct WebSocket, never the daemon process. Request IDs
// are connection-scoped by Codex; no shared stdin stream or proxy is involved.
type daemonSession struct {
	conn       *websocket.Conn
	writeMu    sync.Mutex
	done       chan struct{}
	generation uint64
}

func (s *daemonSession) Write(data []byte) (int, error) {
	s.writeMu.Lock()
	defer s.writeMu.Unlock()
	if err := s.conn.SetWriteDeadline(time.Now().Add(5 * time.Second)); err != nil {
		return 0, err
	}
	if err := s.conn.WriteMessage(websocket.TextMessage, bytes.TrimSuffix(data, []byte{'\n'})); err != nil {
		return 0, err
	}
	return len(data), nil
}

func (s *daemonSession) Close() error {
	// Close this transport only. Never send /daemon/shutdown or any signal.
	return s.conn.Close()
}

func (b *Bridge) connectDaemon() error {
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	var daemonPID int
	dialer := websocket.Dialer{
		HandshakeTimeout: 3 * time.Second,
		NetDialContext: func(ctx context.Context, _, _ string) (net.Conn, error) {
			conn, pid, err := dialExistingDaemon(ctx, b.cfg.socket)
			daemonPID = pid
			return conn, err
		},
	}
	conn, response, err := dialer.DialContext(ctx, "ws://localhost/rpc", nil)
	if response != nil && response.Body != nil {
		_ = response.Body.Close()
	}
	if err != nil {
		return fmt.Errorf("connect existing Codex daemon: %w", err)
	}
	conn.SetReadLimit(8 * 1024 * 1024)
	session := &daemonSession{conn: conn, done: make(chan struct{})}
	b.mu.Lock()
	if b.shuttingDown {
		b.mu.Unlock()
		_ = conn.Close()
		return errors.New("Codex bridge is stopped")
	}
	b.generation++
	session.generation = b.generation
	b.daemon = session
	b.daemonPID = daemonPID
	b.stdin = session
	b.waitDone = session.done
	b.running = true
	b.startedAt = time.Now().UTC()
	b.mu.Unlock()
	go b.readDaemon(session)
	if err := b.initializeProcess(context.Background()); err != nil {
		_ = session.Close()
		<-session.done
		return err
	}
	b.mu.RLock()
	connected := b.daemon == session && !b.shuttingDown
	b.mu.RUnlock()
	if !connected {
		return errors.New("daemon disconnected during initialization")
	}
	b.publish(Event{Time: time.Now().UTC(), Source: "lifecycle", Method: "daemon/connected", Summary: "connected to existing Codex daemon"})
	return nil
}

func (b *Bridge) readDaemon(session *daemonSession) {
	defer close(session.done)
	defer session.Close()
	var readErr error
	for {
		kind, data, err := session.conn.ReadMessage()
		if err != nil {
			readErr = err
			break
		}
		if kind != websocket.TextMessage {
			readErr = errors.New("non-text daemon RPC frame")
			break
		}
		var envelope rpcEnvelope
		if err := json.Unmarshal(data, &envelope); err != nil {
			readErr = errors.New("invalid daemon RPC JSON")
			break
		}
		b.mu.Lock()
		if b.daemon != session {
			b.mu.Unlock()
			return
		}
		// An approval/server request may have the same integer ID as a pending
		// client request. Only a response (no method) can complete that request.
		if id, ok := parseRequestID(envelope.ID); ok && envelope.Method == "" {
			if pending, found := b.pending[id]; found {
				delete(b.pending, id)
				b.mu.Unlock()
				pending.ch <- envelope
				continue
			}
		}
		b.mu.Unlock()
		b.handleEnvelope(envelope, data)
	}
	b.mu.Lock()
	if b.daemon != session {
		b.mu.Unlock()
		return
	}
	for id, pending := range b.pending {
		delete(b.pending, id)
		pending.ch <- rpcEnvelope{Error: &rpcError{Message: "daemon connection lost; request outcome may be unknown; request was not replayed"}}
	}
	b.daemon = nil
	b.daemonPID = 0
	b.stdin = nil
	b.running = false
	b.ready = false
	b.lastExitAt = time.Now().UTC()
	b.thread = nil
	b.turn = nil
	clear(b.threads)
	clear(b.turns)
	stopped := b.shuttingDown
	if !stopped {
		b.lastError = fmt.Sprintf("daemon connection lost: %v", readErr)
	}
	b.mu.Unlock()
	if !stopped {
		b.publish(Event{Time: time.Now().UTC(), Source: "lifecycle", Method: "daemon/disconnected", Summary: "daemon disconnected; pending requests were not replayed"})
		b.startDaemonReconnect()
	}
}

func (b *Bridge) startDaemonReconnect() {
	b.mu.Lock()
	if b.reconnecting || b.shuttingDown {
		b.mu.Unlock()
		return
	}
	b.reconnecting = true
	b.mu.Unlock()
	go func() {
		defer func() {
			b.mu.Lock()
			b.reconnecting = false
			retry := !b.ready && !b.shuttingDown
			b.mu.Unlock()
			if retry {
				b.startDaemonReconnect()
			}
		}()
		for {
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			err := b.EnsureStarted(ctx)
			cancel()
			if err == nil {
				return
			}
			select {
			case <-b.stopCh:
				return
			case <-time.After(time.Second):
			}
		}
	}()
}

func (b *Bridge) requireDaemonThread(threadID string) error {
	if b.cfg.mode != "daemon" {
		return nil
	}
	b.mu.RLock()
	defer b.mu.RUnlock()
	if b.threads[threadID] == nil {
		return errors.New("thread is not owned by this daemon connection; create a new thread after reconnect")
	}
	return nil
}
