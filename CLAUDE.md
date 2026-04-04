# CLAUDE.md — go-stream

## Identity

**Module:** `dappco.re/go/core/stream`
**Repository:** `core/go-stream`
**Tier:** lib (foundation — consumers import this, this never imports consumers)
**Supersedes:** `dappco.re/go/ws` (`core/go-ws`)
**Licence:** EUPL-1.2

## What This Is

Transport-agnostic event and data pipe for the CoreGO ecosystem. Generalises WebSocket, SSE, Redis pub/sub, ZeroMQ, and raw TCP behind a single `Stream` interface. Consumers (`core/api`, `go-pool`, `go-miner`, `go-p2p`, `core/mcp`) call `Stream` — they never import a specific transport.

## Spec

Read the full RFC before touching code:

```
docs/RFC.md                      — go-stream implementation spec (self-contained)
docs/RFC-025-AGENT-EXPERIENCE.md — AX design principles (10 principles)
```

## Build & Test

```bash
go test ./...                            # Run all tests
go test -run TestHub ./...               # Run specific test
go vet ./...                             # Vet
go fmt ./...                             # Format (use Pint equivalent)
```

## Banned Imports

| Banned | Use Instead |
|--------|-------------|
| `fmt` | `core.Sprintf`, `core.Print` |
| `log` | `core.Print`, `core.Error` |
| `errors` | `core.E(scope, message, cause)` |
| `os` | `c.Fs()` |
| `os/exec` | `c.Process()` |
| `strings` | `core.Contains`, `core.TrimPrefix`, etc. |
| `path/filepath` | `core.JoinPath`, `core.PathBase` |
| `encoding/json` | `core.JSONMarshalString`, `core.JSONUnmarshalString` |

## Error Handling

All errors use `core.E(scope, message, cause)`. Never `fmt.Errorf`, never `errors.New`.

## Test Naming

`TestFilename_Function_{Good,Bad,Ugly}` — all three categories mandatory per function.

## Coding Conventions

- UK English (colour, organisation, centre)
- `declare(strict_types=1)` equivalent: all parameters and returns typed
- Comments as usage examples, not prose descriptions (AX principle 2)
- Predictable names over short names (AX principle 1)

## File Map

```
stream.go           — Stream interface, Frame, Channel, Peer, Pipe, Envelope
hub.go              — Hub (central channel-based broker)
hub_config.go       — HubConfig, ChannelAuthoriser, DefaultHubConfig
auth.go             — Authenticator, ConnAuthenticator, built-in implementations
errors.go           — Sentinel errors via core.E()
message.go          — Message, MessageType constants (go-ws compat)
stats.go            — HubStats, per-channel subscriber counts
adapter/ws/         — WebSocket adapter (HTTP upgrade, read/write pumps)
adapter/ws/reconnect.go — ReconnectingClient (client-side reconnecting WS)
adapter/sse/        — SSE adapter (text/event-stream HTTP handler)
adapter/redis/      — Redis pub/sub bridge (echo-safe, envelope pattern)
adapter/zmq/        — ZeroMQ adapter (PUSH/PULL and PUB/SUB)
adapter/tcp/        — Raw TCP adapter (length-prefixed framing)
adapter/tcp/reconnect.go — ReconnectingTCP (client-side reconnecting TCP)
```

## Commit Convention

```
type(scope): description

Co-Authored-By: Virgil <virgil@lethean.io>
```
