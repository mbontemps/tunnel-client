# Shared Codex app-server backend

The full client supports two explicit Codex bridge backends. This does not
change the MCP target, the tunnel/control-plane connection, or MCP credentials.

| Environment | Backend |
| --- | --- |
| unset or `TUNNEL_CLIENT_CODEX_APP_SERVER_MODE=spawn` | Historical supervised `codex app-server` child, JSONL over stdin/stdout. |
| `TUNNEL_CLIENT_CODEX_APP_SERVER_MODE=daemon` | Direct WebSocket connection over an existing, same-user Unix socket. No subprocess, proxy, daemon startup, or fallback to spawn. |

`TUNNEL_CLIENT_CODEX_APP_SERVER_SOCKET` optionally selects an **absolute** Unix
socket path. By default discovery uses
`$CODEX_HOME/app-server-control/app-server-control.sock`, or
`~/.codex/app-server-control/app-server-control.sock`. A daemon manager may
symlink this to its private runtime socket. Both locations must be user-owned
and not group/world-writable; peer credentials are checked after connecting.
macOS and Linux are supported; other platforms fail closed.

`CMD`, `COMMAND`, and `ARGS` overrides are ignored in daemon mode. `CWD` remains
the default **thread** working directory, not a daemon process working
directory. Model, provider, approval policy, sandbox and developer instructions
are sent through the native thread/turn APIs. The daemon's process environment
is shared: do not rely on tunnel-specific environment variables being inherited
by Codex. Configure such requirements through supported request-scoped fields
or the canonical daemon configuration.

## Native architecture contract

Verified against Codex 0.158.0, not an invented stdio multiplexing protocol:

- [Daemon manager and discovery](https://github.com/openai/codex/blob/rust-v0.158.0/codex-rs/app-server-daemon/README.md).
- [Unix WebSocket transport and concurrent accept loop](https://github.com/openai/codex/blob/rust-v0.158.0/codex-rs/app-server-transport/src/transport/unix_socket.rs).
- [Connection-scoped response routing](https://github.com/openai/codex/blob/rust-v0.158.0/codex-rs/app-server/src/outgoing_message.rs).
- [Native protocol](https://learn.chatgpt.com/docs/app-server#protocol): one
  JSON-RPC request/response/notification per WebSocket text frame; each client
  sends `initialize`, then `initialized`. No TCP listener or new auth token is
  introduced by this backend.

Each bridge has its own socket connection and pending-request map. Request IDs
may coincide across bridges without collision. Daemon-wide `thread/started`
broadcasts are filtered by locally owned thread IDs; foreign thread events
must never replace a bridge's thread state. Multiple owned threads are exposed
in `threads` and `turns` snapshot fields. With multiple threads, turn requests
must supply an explicit thread ID. A server request/approval with a colliding
ID cannot complete a client RPC response.

Existing canonical daemon authentication is read without requesting tokens or
refreshing them. Tunnel login/cancellation is disabled in daemon mode so it
cannot replace shared ChatGPT authentication. Stopping a tunnel closes only its
connection; it never signals the daemon or sends a daemon shutdown request.

On transport loss, pending requests fail with an explicit unknown-outcome /
**not replayed** error. The bridge reconnects to the stable discovery socket,
reinitializes, and exposes a new connection generation and peer PID. Ephemeral
threads from the lost connection are discarded: callers must create new ones.
Mutating requests are never automatically retried or replayed. An unavailable
daemon keeps the bridge unready, without silently spawning another backend.

The upstream daemon lifecycle is experimental. Pin and verify protocol
compatibility when upgrading; a shared daemon does not imply per-client
process environment isolation.

## Tests

```sh
go test -race ./pkg/codexappserver
TUNNEL_CLIENT_TEST_DAEMON_SOCKET=/absolute/path/to/existing/socket \
  go test -race ./pkg/codexappserver -run TestNativeDaemonReadOnlyContract -v
```

The opt-in native test only connects, reads account/model metadata, creates two
ephemeral threads without turns, and closes its own connections. It never
starts/restarts the daemon, generates model output, or invokes business tools.
The hermetic suite tests concurrent RPCs/threads, foreign broadcasts, restart
and reconnection, no mutation replay, server-request ID collisions, unsafe
socket rejection, and stopping one bridge while the other remains usable.

Snapshot proof: `mode=daemon`, `pid=0`, `command=""`, `daemon_pid` equal across
tunnels, `ready=true`, and matching `socket_path`. Also inspect OS process trees:
a health response alone cannot prove the absence of a child app-server.

For a native two-**process** tunnel smoke, with a private in-memory MCP fixture:

```sh
node scripts/smoke_shared_codex_daemon.mjs /absolute/tunnel-client /absolute/existing/socket
```

It checks both health/readiness endpoints, MCP initialization/tools list, two
concurrent independent Codex threads, no foreign events/no child app-server,
and that the second tunnel and existing daemon survive stopping the first.
