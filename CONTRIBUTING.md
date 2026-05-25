# Contributing

We treat this repo as "Open Source" within Redis: anyone who clears the bar below is welcome to contribute.

## Local setup

```bash
git clone git@github.com:redis-performance/openstreaming-benchmark.git
cd openstreaming-benchmark
go get -t -v ./...
make build
```

The binary is written to `./openstreaming-benchmark` in the repo root.

Requires **Go 1.19+** (see `go.mod`). No other runtime dependencies — the tool connects to an already-running Redis or compatible streaming backend at test time.

## Branch naming

```
<type>/<short-description>
```

Types: `feat`, `fix`, `refactor`, `test`, `docs`, `chore`

Example: `feat/add-pipeline-mode`

## Coding standards

- Keep changes focused; one logical change per PR.
- Follow the conventions already present in the codebase (formatting, naming, error handling).
- No dead code, no commented-out blocks.

## Submitting changes

1. Fork or create a branch from `main`.
2. Make your changes with clear, atomic commits.
3. Open a pull request against `main` with a descriptive title and summary.
4. Address review comments promptly; force-push to the same branch to update.

## Testing

- All new behaviour must be covered by tests.
- Existing tests must pass: run the test suite locally before opening a PR.
- Coverage should not decrease.

Run the full test suite with:

```bash
make test
```

This is equivalent to:

```bash
GO111MODULE=on go fmt ./...
GO111MODULE=on go test -race -covermode=atomic ./...
```

To generate a coverage report:

```bash
make coverage
```

## Review process

- At least one maintainer approval is required before merge.
- CI must be green.
- Maintainers may request changes or close PRs that do not meet the bar — this is normal and not personal.