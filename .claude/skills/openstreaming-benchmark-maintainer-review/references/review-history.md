# Review history — what was actually found on redis-performance/openstreaming-benchmark

Mined via `gh pr list --repo redis-performance/openstreaming-benchmark --state all --limit 300`,
`gh api repos/redis-performance/openstreaming-benchmark/pulls/<n>/reviews`,
`gh api repos/redis-performance/openstreaming-benchmark/issues/<n>/comments`, and
`gh issue list --repo redis-performance/openstreaming-benchmark --state all` (2026-08-27).
Read this before writing anything, and be honest with yourself about what it says.

**This repo's review record is not "thin like redisbench-admin's" — it is close to empty.**
Do not reach for that comparison as if it implies a similar amount of real signal; there is
markedly less here.

## The whole PR history, in full

15 pull requests total, all merged, all against `main`:

- **14 of 15** were authored by **the same one person**, under two different GitHub identities:
  `filipecosta90` (PRs #1–#13, a personal account) and `fcostaoliveira` (PRs #14–#15, a
  Redis-affiliated account created later). Both resolve to the same human, Filipe Oliveira
  (`gh api users/filipecosta90` and `gh api users/fcostaoliveira` both return "Filipe Oliveira").
  This is, in practice, a solo-maintainer project.
- **PR #15** ("chore: bump GitHub Actions to node24-compatible versions") is the **only PR with
  any review activity at all**: one `APPROVED` review from `paulorsousa`, with **no body text** —
  a silent, drive-by approval. That is the single review event in this repo's entire recorded
  history.
- Every other PR (#1–#14) has **zero reviews and zero PR comments** — `gh api .../reviews` and
  `.../issues/<n>/comments` both return empty arrays for every one of them.
- There are **zero GitHub issues**, open or closed, in this repo's history (`gh issue list
  --state all` returns nothing). There is no issue-triage precedent to mine at all — the
  "triage" workflow below is being added ahead of any real issue ever having been filed, not
  in response to an observed pattern of vague reports.
- PR bodies themselves are typically short, factual, single-sentence-to-a-few-lines summaries
  of the feature added (e.g. PR #6: "Using zipfian distribution for consumers per stream"),
  not structured templates. PR #14 and #15 (the two most recent, authored via the
  `fcostaoliveira` identity) are the only ones with a `## Summary` heading and multiple bullet
  points — a slightly more structured style than the earlier `filipecosta90`-era PRs, but still
  short.
- Outside of PRs, there is exactly one direct-to-history "fix:" commit in `git log`
  (`8d86065 fix: update workflow branch trigger from master to main`) — a CI-workflow fix, not
  a source-code bugfix, and it carries no review text either.

## What this means for the bot's voice

There is no evidenced "maintainer voice" to imitate here beyond: **silence is the normal,
authentic response to a routine PR**, and on the one occasion a second person did review
something, they approved with no comment at all. Do not invent a personality, a set of pet
peeves, or a house style for `fcostaoliveira`/`filipecosta90` or `paulorsousa` — the record
does not support one. If you find yourself writing something that sounds like a specific,
opinionated maintainer voice (the way the memtier_benchmark or redisbench-admin skills can,
because those repos' histories actually contain that), stop — you are fabricating precedent
this repo does not have.

Concretely:

- **Most PRs here should get `skip_comment=true`.** A single-maintainer repo where the
  maintainer already both wrote and merged 14 of the last 15 PRs, same-day, with no review
  friction, is exactly the profile of a project where an AI "first-pass review" adds noise, not
  signal, on anything routine. Reserve an actual comment for something concretely wrong or
  worth a second look — not a rediscovery of what the diff already obviously does.
- When a comment genuinely is warranted, keep it short and factual, matching the terse,
  single-purpose style of this project's own PR descriptions — not a long structured essay,
  and not an imitation of a specific person's turns of phrase, since none were observed.
- Because there is no real precedent for how this maintainer responds to being told something
  is wrong, do not claim or imply "the maintainer usually..." about anything review-behavior
  related. Say plainly, if it comes up, that this repo's own history doesn't give a citable
  example either way.
- For issue triage specifically: since zero issues have ever been filed, there is no example of
  what a real bug report, feature request, or question looks like on this repo, and no
  precedent for what "already actionable" looks like here. Fall back to the generic, conservative
  posture in SKILL.md (ask only for what's concretely missing to reproduce; never claim
  something is a known/duplicate issue without finding and naming the specific one) rather than
  any repo-specific pattern, because there isn't one yet.

## Automated tooling on this repo

Two workflows exist under `.github/workflows/`: `codeql-analysis.yml` (static analysis) and
`publish.yml` (release build/publish on tag). **There is no CI workflow that runs `go test` or
`make test`** — `AGENTS.md` and `CONTRIBUTING.md` both instruct contributors to run
`make test` (`go fmt` + `go test -race -covermode=atomic ./...`) locally before opening a PR,
and a Codecov badge appears in `README.md`, but nothing in `.github/workflows/` actually
executes the test suite or reports coverage on a PR automatically. Do not claim CI enforces
tests or coverage here — the written doctrine exists, but no automated gate for it does. If a
PR under review changes `cmd/*.go` and doesn't mention having run `make test`, that is
genuinely worth a light, factual mention (nothing else will catch it), not an assumption that
some bot already checked.
