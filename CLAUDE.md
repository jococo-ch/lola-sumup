# lola-sumup — Agent Constitution

## What This Project Is

A Rust CLI used in the monthly closing of the Quartiertreffpunkt LoLa in Basel.
It turns the monthly SumUp CSV reports into LoLa-specific Excel files:

- `lola-sumup prepare -s <sales.csv> -t <transactions.csv> <yyyymm>` writes
  `intermediate_<yyyymm>_<ts>.xlsx`, with the columns `Topic`, `Owner`,
  `Purpose` and `Comment` pre-filled by heuristics. Users edit these by hand.
- `lola-sumup export <intermediate.xlsx>` validates the (redacted) file and
  writes the `accounting`, `banana`, `belegskontrolle`, `mittagstisch` and
  `details` exports, each with an extra `transactions` sheet.

The former `close` subcommand lives in the sibling repository `../lola`.

## The README Is the Specification (mandatory reading before any task)

`README.md` is the functional specification: the topic heuristics, the
redaction constraints, every export column with its formula, and the balance
checks on the transitory accounts. Read the section the task touches before
writing any test or code.

The README is the current best description of the intended behaviour, not a
frozen contract. If, while working, you conclude that a heuristic, formula or
column should be different:

1. **Stop.** Do not implement the deviation silently.
2. **Describe** the change and why it serves the bookkeeping better than what
   the README says. If you cannot explain it, follow the README.
3. **Ask** before writing tests or code that reflect it.
4. **If agreed**, update the README first, then implement.

README and code must always describe the same program.

## Layout

| Path | Content |
|---|---|
| `src/main.rs` | clap CLI, month validation, deriving the month from file names |
| `src/prepare.rs` | CSV import, merge of sales and transaction report, Topic/Owner/Purpose heuristics, the domain enums |
| `src/export.rs` | Reads the intermediate xlsx, validates, aggregates, writes all exports |
| `src/export/constraint.rs` | Validation of the redacted columns |
| `src/export/export_details.rs` | Per-day aggregation every other export derives from |
| `src/export/export_*.rs` | One module per export |
| `src/export/posting.rs` | Account pairs for the postings |
| `src/report_log.rs` | German console messages |
| `src/test_fixtures.rs`, `src/test_utils.rs` | rstest fixtures, `assert_dataframe`; test-only |
| `samples/` | Small sample inputs |
| `workdir/` | Real monthly data, gitignored — see Hard Rules |

## Hard Rules

- **`workdir/` and every `*.csv`, `*.xlsx` and `*.xls` outside `samples/` hold
  the association's real transactions.** They are gitignored on purpose.
  - Never commit them, and never quote rows from them in a commit message,
    issue, PR or any external service.
  - Inspecting them to diagnose a problem is allowed. Report counts and
    aggregates back, not individual rows, unless the developer asks for a row.
  - Run the binary on a copy in `/tmp`, never inside the checkout, so no
    output file lands next to the sources.
  - Tests use synthetic data built in `src/test_fixtures.rs`, never a real file.
- **Secrets live only in GitHub** (`SECRETS.adoc`). Never put a token into a
  file, a command line or a log. If a workflow's secrets change, update
  `SECRETS.adoc` in the same change.
- **External web access is read-only** and limited to verifying third-party
  facts: crate versions, library APIs, SumUp report format documentation.
  Never transmit repository contents or financial data to an external service.
- **All code changes must be explainable.** If asked why, you must be able to
  explain.
- **One concern per session and per branch.** Do not mix a feature with an
  unrelated refactoring or dependency bump.
- **Never push and never open a PR** unless the developer asks for it in this
  session.

## What to Do When Unsure

Ask. Do not guess and implement. A clarifying question costs seconds; a wrong
number in the accounting export costs a month's closing.

## Coding Conventions

### Comments — the default is none

Applies to `.rs`, `.yml`, `.toml` and `justfile`.

> **A comment exists to stop a future change causing a regression the code
> cannot make obvious. Nothing else earns one.**

| Already held by | So write |
|---|---|
| A name | A better name |
| Git | Nothing. Why it changed and what it replaced belongs in the commit message |
| The README | One reference to the section, never a paraphrase |
| An assert message | Nothing |

The test for each comment: *what change would this stop someone making?* If
the answer is none, delete it. A comment pointing into another file names a
function, constant or test, never a line number.

Doc comments (`///`) on functions are the exception that stays: they describe
what the function does, in English.

### A name taken from a document needs a body behind it

Domain terms (`Topic`, `Owner`, `Purpose`, `MiTi`, `PaidOut`, account numbers,
column names) come from the README and from SumUp's own headers. Before a term
becomes an identifier or a column name, check that the README body uses it with
the meaning you intend, not only a heading.

Column names are a contract with SumUp's CSV format and with the Excel files
users edit by hand. Renaming one is a breaking change: README, version table
and tests change with it.

### Rust

- Edition 2024, toolchain pinned in `rust-toolchain.toml`.
- `#![warn(clippy::pedantic)]` is on and clippy must be clean.
- No new `unwrap()` or `expect()` in production code; return an error with an
  actionable message instead. Some older call sites remain; this is not
  enforced by a test, so it rests on review.
- Errors are `Result<_, Box<dyn Error>>` or `PolarsResult<_>`.
- Data processing uses polars lazy expressions. Reuse the helpers in
  `export_details.rs` before writing a new aggregation.
- Console output for the user is **German** (`report_log.rs`). Identifiers,
  comments, doc comments and the README are English.

### Tests

- Unit tests live inline in `#[cfg(test)] mod tests` of each module.
- `rstest` for fixtures and case tables, `pretty_assertions` for diffs.
- Shared input frames go into `src/test_fixtures.rs`; compare frames with
  `test_utils::assert_dataframe`.
- A data-driven test's case table is what it asserts, so it stays with the
  test, not with the helpers.

### Dependencies

Dependabot proposes bumps weekly (`.github/dependabot.yml`). When changing a
dependency by hand, check the release notes for breaking changes in polars,
calamine and the Excel writers in particular — they decide the file formats.
Rationale for a non-obvious pin goes as a short comment into `Cargo.toml`, as
the existing `timezones` feature does.

### Markdown

- Never split an inline code span across a line break.
- A GFM table ends at the first line that is not a row. No blank line, no
  paragraph and no list between two rows, and no row with more cells than the
  header — the excess is dropped without a trace in the rendered page.
- Match the wrapping of the file you edit. `README.md` uses long lines; this
  file is wrapped at 80 columns.
- **A section never records two states of one claim.** When you are about to
  write *corrected*, *previously*, *used to*: read the whole section, write
  the position that holds once, and mention the change only where a reader
  would otherwise re-propose the rejected version. History belongs to git.

## TDD — Non-Negotiable Rules

No production code without a failing test that demands it. One test at a time.

**RED**
1. Write the one test that specifies the next behaviour.
2. Run `just test` and confirm it fails **on its assertion**. Not compiling
   counts only as the first red; add a stub that compiles and run again.
3. If the test passes immediately, it is invalid. Stop and ask.

**GREEN**
1. Write the minimal implementation that makes it pass.
2. Run `just test` and confirm all tests pass. Do not refactor yet.

**REFACTOR**
1. Improve naming and structure without changing behaviour.
2. Run `just verify` after every change. Treat clippy's suggestions as
   refactoring candidates, not only as a gate.
3. If a test goes red, revert the last change.

Say these out loud before moving on:

- Before GREEN: "I have observed the test fail with: [exact message]"
- Before REFACTOR: "All N tests pass. The failing test now passes."
- Before the next RED: "All tests are green. Refactor is complete."

### A test's input is never adjusted to make an implementation pass

If an input a real SumUp report would plausibly contain fails, the
implementation is the defect. Record it, propose the fix, ask. The same holds
one level up: a tolerance (the `0.05` balance checks) or a time boundary in
the heuristics is never moved so that a case passes.

### Confirm a test by breaking the code

Before committing a test for a bug fix or a heuristic, revert or disable the
code it protects and watch that test fail. Then restore it.

- **Read which test and which assertion fired**, not only that the run was
  red. A different test failing confirms nothing about the one you wrote.
- **Break one site at a time.** Several at once proves only a joint claim.
- **A mutation that breaks compilation proves little**; find one that compiles.
- **Verify the revert with `git status`**, not by trusting the command ran.
  `git checkout -- <file>` also discards uncommitted work in that file.

## Code Quality Gates (run before declaring any task done)

A gate stated here must match CI's invocation. When CI changes, update this
table in the same change.

| Gate | Command | CI |
|---|---|---|
| Format | `cargo fmt -- --check` | `verify.yml` via `just verify` |
| Lint | `cargo clippy --all-targets` | `verify.yml` via `just verify` |
| Test | `cargo test` | `verify.yml` via `just verify` |
| Coverage | `cargo llvm-cov --locked --lcov --output-path target/lcov.info` | `sonar.yml` |

`just verify` runs the first three and is what to run locally. The coverage
row and the SonarQube Cloud analysis run in CI only; Sonar's OIDC auth cannot
run here.

If `rustc` reports `can't find crate for ...` for a dependency listed in
`Cargo.toml`, `target/` holds stale artefacts, e.g. from a host build. Re-run;
if it persists, `cargo clean`.

## Silencing a Warning

A clippy warning is fixed, or allowed with the reason it cannot be. Never
because it is noisy. An `#[allow(clippy::...)]` is scoped to the one item that
needs it, never to a module or the crate. The existing `too_many_lines` and
`too_many_arguments` allows are the precedent for what is acceptable.

## Never Assert a Fact You Have Not Checked

Nothing goes into the README, a commit message or this file as a fact unless
it came from something run, read or measured in that session: counts, dates,
versions, file names, behaviour. If it cannot be checked cheaply, leave it out.
This applies to figures copied from older text as much as to new ones.

## Documentation Duties

| Changed | Update |
|---|---|
| Export column, formula, heuristic, constraint | The matching `README.md` section |
| Adaptation to a new SumUp report format | `README.md` version table |
| CLI arguments or help text | `README.md` CLI section |
| Workflow secrets | `SECRETS.adoc` |
| A standing fact about working in this repo | This file |

`CHANGELOG.md` is generated by release-plz — never edit it by hand.

## Commit Style and Branching

- Conventional Commits; the PR title is checked (`.github/semantic.yml`).
  Releasing types: `feat`, `fix`, `perf`, `refactor`, `revert`, `sec`
  (`release-plz.toml`). Not released and kept out of the changelog: `chore`,
  `chore(deps)`, `ci`, `docs`, `test`. Breaking changes carry `!`.
- Reference the issue when there is one:
  `fix: [#589] fix determination of late Schichtwechsel`.
- The body says *why*, briefly, in English.
- One logical change per commit.
- Feature and fix work happens on a local branch off `main`, which the
  developer pushes and merges as a PR. Ask before committing to `main`.
- Never bump the version, tag, or edit `release.yml` by hand: release-plz and
  cargo-dist own them. Actions stay pinned by commit SHA with a version
  comment.

## Ending a Session Mid-Task

Nothing survives a session except the repository. Before stopping, the state
must be recoverable from it: the next session, starting from the checklist
below, must reach the same next action without asking. Put it in commit
messages (including an observed failure) or, for a standing fact, in this
file. Prefer to stop between TDD cycles, not inside one. Record negative
results too — "this cannot work, because …" saves the next session from
re-deriving it.

## Project Knowledge vs. Personal Notes

If a note is true for anyone who checks out this project, it belongs in this
file, the README or a commit message — not in an agent's private memory, where
no reviewer sees it and no second developer inherits it.

## Sandbox Notes

The general sandbox rules are in `~/.config/opencode/AGENTS.md`. For this
project:

- Rust and `just` are installed; if a tool is missing from `PATH`, use
  `bash -l -c "..."`.
- Builds need crates.io. On an HTTP 403, follow the network-policy section of
  the global file.
- The first build compiles polars and takes minutes; use long timeouts.

## Session Startup Checklist

1. Read this file.
2. Read the `README.md` section the task touches.
3. `git status` and `git branch`: clean tree, right branch. For new feature or
   fix work, create a branch off `main`.
4. Run `just verify` to establish a green baseline before changing anything.
