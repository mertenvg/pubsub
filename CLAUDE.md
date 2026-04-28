# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

Go pubsub library providing a Gin-inspired API for event-driven messaging. Supports publish/subscribe with middleware chains, plugins, and swappable transport providers.

Module: `github.com/mertenvg/pubsub` (Go 1.21)

## Build & Test Commands

```bash
go build ./...        # Build all packages
go test ./...         # Run all tests
go test -run TestName ./path/to/pkg  # Run a single test
go vet ./...          # Lint
```

## Architecture

**Core types** (root package):
- `Service` — central orchestrator: manages publish/subscribe, worker pool (default 10), middleware chain, plugins, and provider lifecycle. Auto-starts on first `Subscribe()` call.
- `Context` — per-message context (embeds `context.Context`), passed through the handler chain. Provides `Bind()` for deserialization (protobuf, JSON, custom `Unmarshaler`), key-value metadata store, and `Ack()`/`Nack()` with hook support.
- `HandlerFunc func(ctx *Context) error` — handler signature used for both subscribers and middleware. Middleware calls `ctx.Next()` to continue the chain.
- `Group` — middleware grouping for subscriptions (like Gin's `RouterGroup`).

**Interfaces** (`interfaces.go`):
- `Provider` = `Publisher` + `Subscriber` — transport abstraction
- `Plugin` — lifecycle-aware component with its own middleware (Start/Stop/Middleware)
- `Message` — provider-level message with Key/Data/Ack/Nack
- `Marshaler`/`Unmarshaler` — custom serialization

**Configuration** uses functional options: `ServiceOption` for `Service`, `ContextOption` for `Context`.

**Publish flow**: value marshaled (Marshaler → protobuf → string → JSON fallback), key auto-generated if empty, retry with exponential backoff (default 7 attempts).

**Subscribe flow**: messages queued to a buffered channel, dispatched by worker goroutines, each running through middleware + handler chain.

**Providers** (`providers/`):
- `kafka` — Kafka via segmentio/kafka-go with AWS MSK IAM auth
- `memory` — in-memory provider for testing

**Middleware** (`middleware/`): `Logrus` (logging), `Recover` (panic recovery)

**Hooks** (`hooks/`): `Prometheus` (publish hook for metrics)

**Plugins** (`plugins/`): `retry` — dead-letter/retry queue plugin

## Engineering Guidelines

Refer to the AI engineering guidelines at https://github.com/mertenvg/my-ai-guidelines/guidelines/
for the full set of rules. The universal and Go-specific guidelines apply to all work in this repository. Key points are summarized below.

### Core Principles

- **Correctness over optimization.** Working code first; optimize only when measured.
- **Readability over cleverness.**
- **Small, safe changes over large refactors.** Minimize blast radius.
- **Preserve existing architecture** unless explicitly instructed otherwise.
- **Follow existing patterns** before introducing new ones.
- Changes must be minimal, localized, and backwards compatible unless instructed otherwise.
- No drive-by refactors, style-only rewrites, or adding comments/docstrings to unchanged code.

### Repository Awareness

Before writing any code, inspect nearby files and similar implementations. Reuse existing utilities, helpers, and abstractions. Match established naming conventions, folder structure, and dependency patterns. Do not introduce new frameworks or abstractions without clear necessity.

### Go Conventions

- Code must pass `gofmt` and `go vet`. Run repository linters if present.
- Import order: standard library, third-party, local packages.
- Always return errors with context: `fmt.Errorf("context: %w", err)`. Use `%w` for wrapping. No panics for expected failures.
- Use sentinel errors for common conditions; check with `errors.Is()` / `errors.As()`.
- Define interfaces at the consumption boundary, not alongside the implementation. Keep interfaces small (1-3 methods).
- Avoid global mutable state. Use dependency injection via constructors.
- Use `context.Context` for request lifecycles. Propagate cancellation.
- Every goroutine must have a clear shutdown path. Bound concurrency with worker pools or semaphores.
- Code must be race-detector safe (`go test -race`).
- Method names should not repeat the type or package name.

### Testing

- Add/update tests when behavior changes, bugs are fixed, or security-sensitive logic is modified.
- Test behavior, not implementation details. Tests must be deterministic.
- Prefer table-driven tests. Use fakes/stubs over mocks when possible.
- Use `require` for preconditions (abort on failure), `assert` for value checks (continue on failure).
- Name tests clearly: `TestThing_Scenario_Expectation`.
- Verify locally before committing: `go test ./...` and `go vet ./...`.

### Security (Non-Negotiable)

- Never log secrets, credentials, tokens, or private keys.
- Never expose sensitive data in error messages or API responses.
- Validate and sanitize all user input at system boundaries.
- No unsafe deserialization or `eval`-style execution. Never disable TLS verification.

### Performance

- No unbounded concurrency, infinite retries, or unbounded memory growth.
- Timeouts for all network calls. Propagate `context.Context` and respect cancellation.

### Dependencies

- Prefer standard library solutions. Do not add dependencies for trivial functionality.
- Reuse existing project dependencies before adding new ones.

### Git Workflow

- Follow Conventional Commits: `feat:`, `fix:`, `docs:`, `chore:`, `refactor:`.
- Imperative mood, first line under 72 characters.
- One logical change per PR, keep PRs small and focused.

### AI Behavior

- Do not invent requirements, assume undocumented infrastructure, or add features beyond what was requested.
- If a request is ambiguous, ask for clarification before writing code.
- A change is complete only when it compiles, passes tests/linting, handles errors correctly, and is minimal and consistent with the existing codebase.
