# Agent guidelines

Instructions for AI coding agents (Claude Code, Copilot, Cursor, etc.) working in this repo.

## Project overview

`openstreaming-benchmark` is a Go CLI tool that makes it easy to benchmark distributed streaming systems. It drives concurrent producer and consumer workloads against a target (e.g. Redis Streams), measures throughput and latency using HDR histograms, and reports results in a structured format. The project is rated **5 stars** for importance: it is the canonical performance reference for Redis streaming use-cases and is used directly by the Redis Performance team.

## Local setup

```bash
git clone git@github.com:redis-performance/openstreaming-benchmark.git
cd openstreaming-benchmark
go get -t -v ./...
make build
```

The compiled binary lands at `./openstreaming-benchmark` in the repo root. Requires **Go 1.19+** (see `go.mod`). No other runtime dependencies — connect the tool to an already-running Redis or compatible streaming backend at test time.

## Branch naming

Same as human contributors: `<type>/<short-description>` (e.g. `fix/off-by-one-in-pipeline`).

## Coding standards

- Match the style already in the file you are editing.
- Prefer clear, minimal changes over large refactors unless explicitly asked.
- Do not add comments that describe *what* the code does — only add comments when the *why* is non-obvious.
- Do not introduce new dependencies without checking with the maintainer.

## Running tests

Run the full test suite before declaring a task complete:

```bash
make test
```

This formats all Go source files and then runs every test with the race detector enabled:

```bash
GO111MODULE=on go fmt ./...
GO111MODULE=on go test -race -covermode=atomic ./...
```

To produce a coverage report:

```bash
make coverage
```

Always run tests before declaring a task complete.

## How to submit changes

1. Create a branch: `git checkout -b <type>/<description>`.
2. Commit with a clear message focused on *why*, not *what*.
3. Open a pull request against `main`.
4. Do **not** push directly to `main`.

## What to avoid

- Do not reformat files unrelated to your change.
- Do not remove error handling or tests.
- Do not commit secrets, credentials, or large binary files.
- Do not amend published commits.