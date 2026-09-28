//go:build darwin || linux

package codexappserver

import (
	"context"
	"encoding/json"
	"fmt"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/gorilla/websocket"
	"github.com/stretchr/testify/require"
)

type fakeDaemonClient struct {
	conn *websocket.Conn
	mu   sync.Mutex
	name string
}

func (c *fakeDaemonClient) send(v any) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.conn.WriteJSON(v)
}

type heldDaemonRequest struct {
	client *fakeDaemonClient
	id     any
	params map[string]any
}

type fakeSharedDaemon struct {
	t        *testing.T
	socket   string
	mu       sync.Mutex
	server   *http.Server
	listener net.Listener
	clients  map[*fakeDaemonClient]bool
	sequence int
	held     chan heldDaemonRequest
	methods  map[string]int
}

func newFakeSharedDaemon(t *testing.T) *fakeSharedDaemon {
	t.Helper()
	// Keep AF_UNIX addresses short on macOS, including when TMPDIR is long.
	dir, err := os.MkdirTemp("", "cds-")
	require.NoError(t, err)
	if len(dir) > 65 {
		require.NoError(t, os.RemoveAll(dir))
		dir, err = os.MkdirTemp("/tmp", "cds-")
		require.NoError(t, err)
	}
	d := &fakeSharedDaemon{t: t, socket: filepath.Join(dir, "rpc.sock"), clients: map[*fakeDaemonClient]bool{}, methods: map[string]int{}, held: make(chan heldDaemonRequest, 10)}
	d.start()
	t.Cleanup(func() { d.stop(); require.NoError(t, os.RemoveAll(dir)) })
	return d
}

func (d *fakeSharedDaemon) start() {
	listener, err := net.Listen("unix", d.socket)
	require.NoError(d.t, err)
	require.NoError(d.t, os.Chmod(d.socket, 0600))
	server := &http.Server{Handler: http.HandlerFunc(d.serve)}
	d.mu.Lock()
	d.listener = listener
	d.server = server
	d.mu.Unlock()
	go func() { _ = server.Serve(listener) }()
}

func (d *fakeSharedDaemon) stop() {
	d.mu.Lock()
	server, listener := d.server, d.listener
	d.server = nil
	d.listener = nil
	clients := make([]*fakeDaemonClient, 0, len(d.clients))
	for c := range d.clients {
		clients = append(clients, c)
	}
	d.mu.Unlock()
	for _, c := range clients {
		_ = c.conn.Close()
	}
	if server != nil {
		_ = server.Close()
	}
	if listener != nil {
		_ = listener.Close()
	}
}

func (d *fakeSharedDaemon) broadcast(v any) {
	d.mu.Lock()
	clients := make([]*fakeDaemonClient, 0, len(d.clients))
	for c := range d.clients {
		clients = append(clients, c)
	}
	d.mu.Unlock()
	for _, c := range clients {
		_ = c.send(v)
	}
}

func (d *fakeSharedDaemon) serve(w http.ResponseWriter, r *http.Request) {
	upgrader := websocket.Upgrader{}
	conn, err := upgrader.Upgrade(w, r, nil)
	if err != nil {
		return
	}
	c := &fakeDaemonClient{conn: conn}
	d.mu.Lock()
	d.clients[c] = true
	d.mu.Unlock()
	defer func() { _ = conn.Close(); d.mu.Lock(); delete(d.clients, c); d.mu.Unlock() }()
	for {
		var message rpcEnvelope
		if err := conn.ReadJSON(&message); err != nil {
			return
		}
		var params map[string]any
		_ = json.Unmarshal(message.Params, &params)
		d.mu.Lock()
		d.methods[message.Method]++
		d.mu.Unlock()
		var result any = map[string]any{}
		switch message.Method {
		case "initialize":
			info, _ := params["clientInfo"].(map[string]any)
			c.name = stringValue(info["name"])
			result = map[string]any{"userAgent": "mock/0.158.0 " + c.name, "platformFamily": "unix", "platformOs": "test", "codexHome": "/canonical"}
		case "initialized":
			continue
		case "getAuthStatus":
			result = map[string]any{"authMethod": "chatgpt"}
		case "account/read":
			result = map[string]any{"account": map[string]any{"type": "chatgpt", "email": "daemon@example.test"}, "requiresOpenaiAuth": true}
		case "thread/start":
			d.mu.Lock()
			d.sequence++
			id := fmt.Sprintf("thread_%d", d.sequence)
			d.mu.Unlock()
			thread := map[string]any{"id": id, "cwd": params["cwd"], "createdAt": 1, "updatedAt": 1}
			// Codex broadcasts this even to clients which did not start the thread.
			d.broadcast(map[string]any{"method": "thread/started", "params": map[string]any{"thread": thread}})
			result = map[string]any{"thread": thread, "model": params["model"], "approvalPolicy": params["approvalPolicy"], "sandbox": map[string]any{"type": params["sandbox"]}}
		case "turn/start":
			thread := params["threadId"].(string)
			id := "turn_" + thread
			for _, status := range []string{"inProgress", "completed"} {
				method := "turn/started"
				if status == "completed" {
					method = "turn/completed"
				}
				d.broadcast(map[string]any{"method": method, "params": map[string]any{"threadId": thread, "turn": map[string]any{"id": id, "status": status}}})
			}
			result = map[string]any{"turn": map[string]any{"id": id, "status": "inProgress"}}
		case "hold":
			d.held <- heldDaemonRequest{client: c, id: message.ID, params: params}
			continue
		case "echo":
			id, value := message.ID, params["value"]
			// Responses deliberately arrive out of order on one connection.
			go func() {
				time.Sleep(time.Duration(int(value.(float64))%5) * time.Millisecond)
				_ = c.send(map[string]any{"id": id, "result": map[string]any{"value": value}})
			}()
			continue
		}
		if message.ID != nil {
			if err := c.send(map[string]any{"id": message.ID, "result": result}); err != nil {
				return
			}
		}
	}
}

func (d *fakeSharedDaemon) bridge(t *testing.T) *Bridge {
	t.Helper()
	b := NewBridgeWithLookupEnv(nil, nil, func(k string) (string, bool) {
		values := map[string]string{"TUNNEL_CLIENT_CODEX_APP_SERVER_MODE": "daemon", "TUNNEL_CLIENT_CODEX_APP_SERVER_SOCKET": d.socket, "TUNNEL_CLIENT_CODEX_APP_SERVER_COMMAND": "must-never-be-run", "TUNNEL_CLIENT_CODEX_APP_SERVER_CMD": "/does/not/exist", "TUNNEL_CLIENT_CODEX_APP_SERVER_CWD": "/client/default"}
		v, ok := values[k]
		return v, ok
	})
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	require.NoError(t, b.EnsureStarted(ctx))
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
		defer cancel()
		require.NoError(t, b.Stop(ctx))
	})
	return b
}

func TestDaemonTwoBridgesThreadsAndConcurrentRequests(t *testing.T) {
	d := newFakeSharedDaemon(t)
	a, b := d.bridge(t), d.bridge(t)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	for _, bridge := range []*Bridge{a, b} {
		s := bridge.Snapshot()
		require.Equal(t, "daemon", s.Mode)
		require.Equal(t, 0, s.PID)
		require.Equal(t, os.Getpid(), s.DaemonPID)
		require.Empty(t, s.Command)
		require.True(t, s.Ready)
		require.Contains(t, s.InitializeInfo.UserAgent, "0.158.0")
		require.Equal(t, "daemon@example.test", s.Account.Email)
		bridge.mu.RLock()
		require.Nil(t, bridge.cmd)
		bridge.mu.RUnlock()
	}
	at, err := a.StartThread(ctx, ThreadStartParams{CWD: "/a", Model: "model-a", SandboxType: "read-only", ApprovalPolicy: "never"})
	require.NoError(t, err)
	bt, err := b.StartThread(ctx, ThreadStartParams{CWD: "/b", Model: "model-b", SandboxType: "workspace-write", ApprovalPolicy: "on-request"})
	require.NoError(t, err)
	at2, err := a.StartThread(ctx, ThreadStartParams{Model: "model-a2", SandboxType: "read-only", ApprovalPolicy: "never"})
	require.NoError(t, err)
	require.Equal(t, "/client/default", at2.CWD)
	require.NotEqual(t, at.ThreadID, bt.ThreadID)
	require.Equal(t, "model-a", at.Model)
	require.Equal(t, "never", at.ApprovalPolicy)
	require.Equal(t, "on-request", bt.ApprovalPolicy)
	require.Equal(t, "workspace-write", bt.Sandbox)
	require.Len(t, a.Snapshot().Threads, 2)
	require.Len(t, b.Snapshot().Threads, 1)
	_, err = a.StartTurn(ctx, TurnStartParams{ThreadID: bt.ThreadID})
	require.ErrorContains(t, err, "not owned")
	_, err = a.StartTurn(ctx, TurnStartParams{})
	require.ErrorContains(t, err, "explicit thread id")
	var wg sync.WaitGroup
	for i := 0; i < 40; i++ {
		for clientIndex, bridge := range []*Bridge{a, b} {
			wg.Add(1)
			go func(bridge *Bridge, value int) {
				defer wg.Done()
				raw, err := bridge.request(ctx, "echo", map[string]any{"value": value})
				if !assertNoError(t, err) {
					return
				}
				var got struct {
					Value int `json:"value"`
				}
				if !assertNoError(t, json.Unmarshal(raw, &got)) {
					return
				}
				if got.Value != value {
					t.Errorf("mixed response: got %d want %d", got.Value, value)
				}
			}(bridge, (clientIndex+1)*1000+i)
		}
	}
	wg.Wait()
	for _, pair := range []struct {
		bridge *Bridge
		thread ThreadStartResult
	}{{a, at}, {a, at2}, {b, bt}} {
		wg.Add(1)
		go func() {
			defer wg.Done()
			turn, err := pair.bridge.StartTurn(ctx, TurnStartParams{ThreadID: pair.thread.ThreadID})
			if assertNoError(t, err) && turn.Status != "completed" {
				t.Errorf("terminal status overwritten: %s", turn.Status)
			}
		}()
	}
	wg.Wait()
	require.Len(t, a.Snapshot().Turns, 2)
	require.Len(t, b.Snapshot().Turns, 1)
	for _, event := range a.RecentEvents(100) {
		require.NotEqual(t, bt.ThreadID, event.ThreadID)
	}
	for _, event := range b.RecentEvents(100) {
		require.NotEqual(t, at.ThreadID, event.ThreadID)
		require.NotEqual(t, at2.ThreadID, event.ThreadID)
	}
	require.NoError(t, a.Stop(ctx))
	require.True(t, b.Snapshot().Ready)
	_, err = b.request(ctx, "echo", map[string]any{"value": 99})
	require.NoError(t, err)
	d.mu.Lock()
	require.Equal(t, 0, d.methods["account/login/start"])
	d.mu.Unlock()
}

func assertNoError(t *testing.T, err error) bool {
	t.Helper()
	if err != nil {
		t.Error(err)
		return false
	}
	return true
}

func TestDaemonReconnectFailsPendingWithoutReplayOrStaleThreads(t *testing.T) {
	d := newFakeSharedDaemon(t)
	b := d.bridge(t)
	ctx, cancel := context.WithTimeout(context.Background(), 8*time.Second)
	defer cancel()
	thread, err := b.StartThread(ctx, ThreadStartParams{Model: "model", SandboxType: "read-only", ApprovalPolicy: "never"})
	require.NoError(t, err)
	errors := make(chan error, 1)
	go func() { _, err := b.request(ctx, "hold", nil); errors <- err }()
	<-d.held
	generation := b.Snapshot().ConnectionGeneration
	d.stop()
	require.ErrorContains(t, <-errors, "not replayed")
	d.start()
	require.Eventually(t, func() bool { return b.Snapshot().Ready && b.Snapshot().ConnectionGeneration > generation }, 5*time.Second, 20*time.Millisecond)
	require.Empty(t, b.Snapshot().Threads)
	_, err = b.StartTurn(ctx, TurnStartParams{ThreadID: thread.ThreadID})
	require.ErrorContains(t, err, "not owned")
	_, err = b.StartThread(ctx, ThreadStartParams{Model: "new", SandboxType: "read-only", ApprovalPolicy: "never"})
	require.NoError(t, err)
	d.mu.Lock()
	require.Equal(t, 1, d.methods["hold"])
	d.mu.Unlock()
}

func TestDaemonServerRequestDoesNotConsumeCollidingResponseID(t *testing.T) {
	d := newFakeSharedDaemon(t)
	b := d.bridge(t)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	thread, err := b.StartThread(ctx, ThreadStartParams{ApprovalPolicy: "on-request"})
	require.NoError(t, err)
	result := make(chan json.RawMessage, 1)
	go func() {
		raw, err := b.request(ctx, "hold", nil)
		if err != nil {
			t.Error(err)
		}
		result <- raw
	}()
	held := <-d.held
	require.NoError(t, held.client.send(map[string]any{"id": held.id, "method": "item/commandExecution/requestApproval", "params": map[string]any{"threadId": thread.ThreadID}}))
	require.Eventually(t, func() bool {
		for _, e := range b.RecentEvents(20) {
			if e.Method == "item/commandExecution/requestApproval" {
				return true
			}
		}
		return false
	}, time.Second, 10*time.Millisecond)
	select {
	case <-result:
		t.Fatal("server request consumed pending response")
	default:
	}
	require.NoError(t, held.client.send(map[string]any{"id": held.id, "result": map[string]any{"ok": true}}))
	require.JSONEq(t, `{"ok":true}`, string(<-result))
}

func TestDaemonFailsClosedNeverSpawnsAndDoesNotOwnAuthentication(t *testing.T) {
	d := newFakeSharedDaemon(t)
	b := d.bridge(t)
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	_, err := b.StartDeviceCodeLogin(ctx)
	require.ErrorContains(t, err, "canonically")
	require.ErrorContains(t, b.CancelLogin(ctx, "foreign-login"), "canonically")
	require.NoError(t, b.Stop(ctx))
	require.ErrorContains(t, b.EnsureStarted(ctx), "stopped")
	missing := NewBridgeWithLookupEnv(nil, nil, func(k string) (string, bool) {
		m := map[string]string{"TUNNEL_CLIENT_CODEX_APP_SERVER_MODE": "daemon", "TUNNEL_CLIENT_CODEX_APP_SERVER_SOCKET": d.socket + ".absent", "TUNNEL_CLIENT_CODEX_APP_SERVER_COMMAND": "must-never-be-executed"}
		v, ok := m[k]
		return v, ok
	})
	short, shortCancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer shortCancel()
	require.Error(t, missing.EnsureStarted(short))
	require.Equal(t, 0, missing.Snapshot().PID)
	require.Empty(t, missing.Snapshot().Command)
	require.Nil(t, missing.cmd)
	require.NoError(t, missing.Stop(ctx))
	invalid := NewBridgeWithLookupEnv(nil, nil, func(k string) (string, bool) {
		if k == "TUNNEL_CLIENT_CODEX_APP_SERVER_MODE" {
			return "invalid", true
		}
		return "", false
	})
	require.ErrorContains(t, invalid.EnsureStarted(ctx), "unsupported")
	require.Nil(t, invalid.cmd)
}

func TestDaemonRejectsUnsafeSocket(t *testing.T) {
	d := newFakeSharedDaemon(t)
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	require.NoError(t, os.Chmod(d.socket, 0666))
	_, _, err := dialExistingDaemon(ctx, d.socket)
	require.ErrorContains(t, err, "not group/world-writable")
	require.NoError(t, os.Chmod(d.socket, 0600))
	_, _, err = dialExistingDaemon(ctx, "relative.sock")
	require.ErrorContains(t, err, "absolute")
}

// Opt-in protocol contract test against an ALREADY RUNNING daemon. It does not
// start/restart a process, modify authentication, start turns, or invoke tools.
func TestNativeDaemonReadOnlyContract(t *testing.T) {
	socket := os.Getenv("TUNNEL_CLIENT_TEST_DAEMON_SOCKET")
	if socket == "" {
		t.Skip("set TUNNEL_CLIENT_TEST_DAEMON_SOCKET to test an existing daemon")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	bridges := make([]*Bridge, 2)
	for i := range bridges {
		b := NewBridgeWithLookupEnv(nil, nil, func(k string) (string, bool) {
			values := map[string]string{"TUNNEL_CLIENT_CODEX_APP_SERVER_MODE": "daemon", "TUNNEL_CLIENT_CODEX_APP_SERVER_SOCKET": socket}
			v, ok := values[k]
			return v, ok
		})
		bridges[i] = b
		t.Cleanup(func() {
			stop, done := context.WithTimeout(context.Background(), time.Second)
			defer done()
			require.NoError(t, b.Stop(stop))
		})
		require.NoError(t, b.EnsureStarted(ctx))
		require.True(t, b.Snapshot().Ready)
		require.Zero(t, b.Snapshot().PID)
		require.Empty(t, b.Snapshot().Command)
		require.Nil(t, b.cmd)
	}
	require.Greater(t, bridges[0].Snapshot().DaemonPID, 0)
	require.Equal(t, bridges[0].Snapshot().DaemonPID, bridges[1].Snapshot().DaemonPID)
	var wg sync.WaitGroup
	results := make([]ThreadStartResult, 2)
	for i, b := range bridges {
		wg.Add(1)
		go func() {
			defer wg.Done()
			thread, err := b.StartThread(ctx, ThreadStartParams{CWD: os.TempDir(), Model: "gpt-6-sol", SandboxType: "read-only", ApprovalPolicy: "never"})
			if assertNoError(t, err) {
				results[i] = thread
			}
		}()
	}
	wg.Wait()
	require.NotEmpty(t, results[0].ThreadID)
	require.NotEqual(t, results[0].ThreadID, results[1].ThreadID)
	for _, b := range bridges {
		require.Len(t, b.Snapshot().Threads, 1)
		for _, e := range b.RecentEvents(100) {
			if e.ThreadID != "" {
				require.Equal(t, b.Snapshot().Threads[0].ID, e.ThreadID)
			}
		}
	}
	require.NoError(t, bridges[0].Stop(ctx))
	_, err := bridges[1].request(ctx, "model/list", map[string]any{"limit": 1})
	require.NoError(t, err)
	t.Logf("same daemon PID=%d; independent ephemeral threads; no turns/tools/auth writes", bridges[1].Snapshot().DaemonPID)
}
