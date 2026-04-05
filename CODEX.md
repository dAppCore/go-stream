# CODEX.md — go-stream

This repository keeps its working conventions in [CLAUDE.md](/workspace/CLAUDE.md).

Read these two documents before changing code:

```text
docs/RFC.md                      — go-stream implementation spec
docs/RFC-025-AGENT-EXPERIENCE.md — AX design principles
```

Key conventions:

- Use `core.E(scope, message, cause)` for errors.
- Keep comments as concrete usage examples.
- Prefer predictable names over shorthand.
- Preserve the transport-agnostic public API and the `ws` compatibility surface.

Commit convention:

```text
type(scope): description

Co-Authored-By: Virgil <virgil@lethean.io>
```
