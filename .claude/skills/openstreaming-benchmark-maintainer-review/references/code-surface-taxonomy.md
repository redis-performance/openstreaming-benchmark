# Code-surface checklist — grounded in this repo's actual source, not reviewer precedent

`review-history.md` covers what this repo's real PR/issue *review comments* show (almost
nothing). This file is different: since there is no reviewer-comment precedent to mine, the
items below are grounded instead in **what the actual source code in
`redis-performance/openstreaming-benchmark` looks like today** (read directly, not inferred),
plus this repo's own written `AGENTS.md`/`CONTRIBUTING.md`. Cite these as "here's what this
codebase's real architecture looks like," never as "a reviewer has caught this before" — no
reviewer ever has, on the record.

The whole repo is small: `main.go` plus six files under `cmd/` (`root.go`, `common.go`,
`producer.go`, `consumer.go`, `test_result.go`, `bin_info.go`), about 935 lines of Go total at
time of writing. Read the actual file(s) a PR touches before applying any of this — don't
assume a pattern below still holds if the code has since changed.

1. **Shared, package-level HDR histogram state written from multiple goroutines.**
   `producer.go` and `consumer.go` both declare package-level `var latencies
   *hdrhistogram.Histogram` / `var latenciesTick *hdrhistogram.Histogram`, populated from
   worker goroutines started via `go func() { ... }()` and coordinated with a `sync.WaitGroup`.
   `hdrhistogram-go`'s `Histogram` is not documented as safe for concurrent writes from
   multiple goroutines without external synchronization. If a PR adds a new goroutine that
   records into one of these histograms, or changes how/when they're read (e.g. for a new
   per-tick metric), check whether the existing single-writer-per-histogram-instance pattern
   still holds, or whether the change introduces a real data race — `make test` runs
   `go test -race`, so this is exactly the kind of bug that test suite exists to catch, but only
   if a test actually exercises the new concurrent path.

2. **Error handling here is `panic(err)`, not returned errors, at several call sites** (e.g.
   `common.go`'s `getRedisClusterAndConnectedNodes`/related helpers, `consumer.go`'s consumer
   setup). This is the existing convention for unrecoverable setup-time failures (bad host,
   failed connection), not a nitpick to relitigate — but a new code path that panics on
   something that's actually a normal, expected runtime condition (e.g. a transient network
   error mid-benchmark, or a malformed but user-supplied flag value) rather than a genuine setup
   failure is worth naming, since it would take down the whole benchmark run rather than
   degrading or reporting per-connection.

3. **`context.Background()` is used at every call site seen** (`producer.go`, `consumer.go`,
   `common.go`'s DNS resolution) — there is no cancellation or deadline propagated into any
   blocking Redis/network call. This is consistent throughout the current code, so it is not
   itself a bug to flag on an unrelated PR — but a new PR that adds a new blocking call (a new
   command, a new setup step) and doesn't have a way to be interrupted (e.g. on the tool's own
   shutdown signal, which `consumer.go` does otherwise handle via a `chan os.Signal`) is worth a
   light, factual mention, especially if it could block the whole run.

4. **New CLI flags are this project's single most common kind of change** — 13 of the first 14
   merged PRs added or extended a CLI flag/behavior (e.g. `--consumers-per-stream-min/-max`,
   `--read-buffer-each-conn`, `--client-keepalive`, `wait`/`waitReplicas`). `root.go` wires flags
   via cobra (`StringVar`/`IntVar`/etc.); `common.go` and the producer/consumer entry points
   consume them. For a PR adding a new flag, the practical, groundable things to check are:
   whether the flag has a sane, stated default (most existing flags do — e.g. keepalive default
   "60 secs" is called out in the PR #3 title itself), whether it's actually wired through to
   every code path implied by its name (producer *and* consumer, if applicable), and whether
   `README.md`/`AGENTS.md`/`CONTRIBUTING.md` need a corresponding update — this project has no
   separate flag-reference doc, so the flag's own `--help` text in `root.go` is usually the only
   documentation of what it does.

5. **No CI workflow runs the test suite.** See `review-history.md`'s "Automated tooling" section
   — `codeql-analysis.yml` and `publish.yml` are the only workflows; neither runs `make test` or
   `go test`. `CONTRIBUTING.md` states "All new behaviour must be covered by tests" and
   "Existing tests must pass: run the test suite locally before opening a PR," but nothing
   automated verifies this actually happened before merge. This is real, written doctrine with
   zero automated enforcement — worth a light, factual mention if a non-trivial `cmd/*.go`
   change ships with no corresponding test file change and the PR description doesn't mention
   having run `make test`, but don't claim CI would have caught it, because nothing does.

## What this taxonomy is honestly silent on

- **No merged bugfix commit of any kind exists in this repo's history** (the only "fix:"
  commit found, `8d86065`, is a CI YAML branch-name fix, not a source-code bug). Unlike
  redisbench-admin (which has real, citable merged bugfixes for metrics-filtering,
  mutually-exclusive-flag, and opt-in-flag-defeated-by-later-code bugs), this repo has never
  had a source-code bug fixed on the record to point to. Do not invent one. If a PR's change
  resembles a "class" of bug in the abstract, reason about it on first principles and say so —
  do not claim it echoes a specific incident here, because none exists.
- **Retry/backoff behavior.** No retry or backoff logic was found anywhere in the current
  source at time of writing, and no PR has ever added or discussed one. If a PR introduces
  retry/backoff, there is no repo precedent to lean on for what a sane policy looks like here —
  reason about the worst-case wall-clock cost on its own merits, same as you would for any new
  code, without citing false precedent.
- **Backward-compatible flag/output changes.** No PR in this repo's history has changed the
  meaning of an existing flag or an existing output field — every merged PR strictly adds. If a
  PR under review changes existing behavior rather than adding to it, say plainly that this
  repo's own history doesn't give a citable precedent for how that trade-off was previously
  weighed.
- **Memory-safety/buffer-sizing nitpicks** in the C-string sense do not apply — this is Go, not
  C/C++. Do not import memtier_benchmark's `snprintf`/buffer-sizing category here.
