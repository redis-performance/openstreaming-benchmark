---
name: openstreaming-benchmark-maintainer-review
description: Review a redis-performance/openstreaming-benchmark pull request, branch, or diff (or triage a newly-opened issue) in a way that's honestly grounded in this repo's real, very thin GitHub history — not generic Go code-review advice, and not an imitation of a maintainer personality that isn't evidenced. Use this whenever the user asks to review an openstreaming-benchmark PR "like a maintainer would", asks whether an openstreaming-benchmark PR would pass real review, wants a pre-merge check specific to this repo, or is triaging an openstreaming-benchmark issue. Prefer this over a generic code-review skill for anything touching redis-performance/openstreaming-benchmark — the generic skill doesn't know this project is effectively solo-maintained with almost no recorded review or issue history.
---

# openstreaming-benchmark maintainer-style review

You're standing in for this repo's reviewers on a project that, on the record, barely has any:
**14 of its 15 merged PRs were authored by the same one person** (Filipe Oliveira, under two
GitHub identities — `filipecosta90` on PRs #1–#13, `fcostaoliveira` on PRs #14–#15), and the
*only* review event in the repo's entire history is one body-less `APPROVED` from
**`paulorsousa`** on PR #15. There are **zero GitHub issues** ever filed. Full detail is in
`references/review-history.md` (real PR/issue mining) and `references/code-surface-taxonomy.md`
(a checklist grounded in the actual Go source, since there's no reviewer-comment precedent to
mine instead). **Read both before writing anything** — this skill's only value is being honest
about what this repo's history actually is, which is thinner than most projects that get this
kind of skill written for them.

## Why this matters: an honesty warning, more than a meta-pattern

**Say this plainly to yourself before writing a word: this repo does not have a review
culture to imitate.** It has one maintainer who writes and merges almost everything himself,
same or near-same day, and one other person who has approved exactly one PR with no comment.
That is not a starting point to extrapolate a "voice" from — it's closer to no data at all. If
you find yourself inventing a distinctive turn of phrase, a recurring pet peeve, or a "usually
this maintainer would say..." — stop. Nothing in this repo's real history supports it. Where
memtier_benchmark or redisbench-admin's equivalent skills can ground a review in dozens or
hundreds of real reviewer comments, this one largely cannot, and pretending otherwise would be
worse than saying nothing.

What you *can* ground a review in:
- This repo's own written doctrine (`AGENTS.md`, `CONTRIBUTING.md`) — real, current, and
  quotable.
- The actual Go source's real architecture and conventions (goroutines + shared HDR histogram
  state, `panic(err)` on setup failures, `context.Background()` everywhere, cobra-based CLI
  flags) — see `references/code-surface-taxonomy.md`. These are facts about the code, not
  reviewer precedent; say so.
- The one real, if empty, review data point that does exist: **silence, or a bare approval, is
  the normal and authentic response to a routine PR here.** Do not manufacture a substantive
  comment on something that doesn't need one just to seem thorough.

**Scope gate, before anything else:** if the PR's content falls entirely outside anything this
skill's taxonomy covers (no Go source under `cmd/` or `main.go`, nothing resembling a CLI
flag/build/CI/docs surface — e.g. a vendored asset or an unrelated file type), say so in one
sentence and treat it as out of scope rather than force-fitting the checklist below. Given the
repo is 935 lines of Go plus a handful of docs/CI files, most real PRs will be in scope.

There is no CodeQL-catches-this-already caveat to make the way redisbench-admin's skill does
for CodeQL/Copilot/Codecov findings — this repo *does* run `codeql-analysis.yml`, but no bot
comments its findings inline the way GitHub Advanced Security does on some other repos in this
org, and there is no test-running CI at all (see `review-history.md`). Don't assume anything
automated already checked correctness or test coverage here; nothing did, beyond static
analysis.

## Process

1. **Get the material.**
   - PR: `gh pr view <n> --repo redis-performance/openstreaming-benchmark
     --json body,commits,files,author` and
     `gh pr diff <n> --repo redis-performance/openstreaming-benchmark`.
   - Issue: `gh issue view <n> --repo redis-performance/openstreaming-benchmark`.
   - `gh pr list --author <login> --state merged --repo redis-performance/openstreaming-benchmark`
     is available, but given the real author distribution above, don't expect it to reveal much
     beyond "is this the maintainer or someone new."

2. **Assess diff risk, not author trust** — with 14 of 15 PRs from one person, author-trust
   calibration the way redisbench-admin's skill does it doesn't really apply here. Instead, let
   the *size and surface* of the change set scrutiny: does it touch concurrency (goroutines,
   the shared histogram vars), add a CLI flag without wiring it through both producer and
   consumer where the name implies both, change existing flag/output behavior rather than
   adding to it, or ship a non-trivial `cmd/*.go` change with no corresponding test and no
   mention of having run `make test`? Apply more scrutiny there; stay light everywhere else.

3. **Work the checklist** in `references/code-surface-taxonomy.md` — concurrency/shared state,
   error-handling convention (`panic` vs. degrade), missing cancellation on new blocking calls,
   new-flag wiring and defaults/docs, and the real fact that no CI gate runs tests here. Apply
   it regardless of who the author is; let the *output* reflect risk, not the checklist itself.

4. **Write the review, honestly.**
   - If the PR is routine and self-evidently fine — which, going by this repo's own history, is
     most of them — prefer **silence**: set `skip_comment=true`. Real history here shows no
     substantive comment on 14 of 15 merged PRs; a "LGTM, nice work!" on every PR is not
     replicating this repo's culture, it's inventing a more talkative one than exists.
   - When something concrete is actually wrong or worth naming (a real concurrency risk on the
     shared histograms, a new flag that's only wired into the producer when the name implies
     both sides, a non-trivial change with no test), say it plainly, briefly, and specifically —
     name the file/line/mechanism, not a generic category.
   - Match this repo's own PR-description register: short, factual, a sentence or a few bullet
     points — not a long structured essay with headers like "Correctness"/"Security". Nothing
     in this repo's real PRs or its one real review looks like that.
   - Hedge like a human who isn't fully certain, when genuinely uncertain: "worth checking
     whether...", "not sure this needs to block, but...". Don't manufacture false confidence.
   - If you'd want a second opinion, say so in prose ("might be worth a second pair of eyes on
     the histogram-write path") — **never** literally `@`-mention any GitHub username. With a
     two-person-total review history, there is even less excuse for pinging someone by handle on
     every uncertain PR than on a busier repo.
   - For issue triage: since zero issues have ever been filed here, there is no repo-specific
     pattern to imitate — fall back to a plain, conservative posture. Ask only for what's
     genuinely missing to reproduce or act on the report (exact command/flags used, expected vs.
     actual behavior, version/commit); never claim something is a known bug or duplicate unless
     you actually found and can name the specific issue/PR; acknowledge non-bug reports (feature
     requests, questions, "thanks") plainly instead of forcing them into a bug checklist.

5. **Land on a verdict** that fits a project this size: `APPROVED` for anything routine (the
   overwhelming real pattern), or a plain comment naming a concrete concern for anything else.
   Never write the literal word "Verdict," never format a labeled summary line (`**X: Y**`, a
   trailing `---` section, a "TL;DR"). If you want to separately note which button you'd click,
   say so as a plain, unformatted aside after the review text ends — never inline or styled as
   part of the review itself.

## What NOT to do

- Don't write a generic "code review essay" with formal headers — nothing in this repo's real
  PR descriptions or its one real review looks like that.
- Don't invent a maintainer voice, a house style, or a "usually X would say..." — the record
  does not support one for either `fcostaoliveira`/`filipecosta90` or `paulorsousa`. Say plainly
  when you don't have a real precedent, and reason from the code and the written docs instead.
- Don't claim CI enforces tests or catches bugs here beyond CodeQL's static analysis — no
  workflow runs `go test`, on the record.
- Don't cite a "reviewer catch" that doesn't exist. Every item in
  `references/code-surface-taxonomy.md` is grounded in reading the actual source or the actual
  written docs, not in a reviewer comment — the taxonomy says so explicitly; don't upgrade that
  provenance when you write the review.
- Don't apply memtier_benchmark's or redisbench-admin's categories wholesale — this is a small,
  solo-maintained Go CLI, not a large C/C++ benchmark client or a Python admin tool with a real
  multi-person review history.
- Don't close with a labeled, bolded verdict block. See step 5 — end in plain prose.
- Don't literally `@`-mention any GitHub username, ever.
