---
name: analyse-dependency
description: Assesses and lands dependency updates in the lola-sumup repo - open Dependabot pull requests and Dependabot security alerts - across the Cargo crates of the CLI and the GitHub Actions of its workflows. Reads changelogs, checks fixed CVEs and bugs against our code, commits what a Dependabot branch needs to pass CI, and reports one assessment per update for the pull request and one release recommendation per batch.
disable-model-invocation: true
argument-hint: [nothing for open Dependabot PRs, a PR number, or alerts followed by pasted alerts]
allowed-tools: Read Grep Glob WebFetch Bash(git:*) Bash(gh:*) Bash(just:*) Bash(cargo:*) Bash(curl:*) Bash(mktemp:*)
---

Task: $ARGUMENTS

This skill is an index and a procedure. The working rules live in
`CLAUDE.md` (Hard Rules, Dependencies, Commit Style); read them there, since a
summary here would drift. What to assess per update is in
[assessment.md](assessment.md).

## Two entry points

| `$ARGUMENTS` | Source | Commits |
|---|---|---|
| empty, or a PR number | open Dependabot pull requests | on the PR's `dependabot/*` branch, if CI needs it |
| `alerts` and pasted alerts | Dependabot security alerts on GitHub | none; the fix is a proposal |

One update per assessment. A grouped PR (`all-actions`, or a security update
"in the cargo group") is assessed per dependency it moves, with one CI
section for the PR.

## Where each ecosystem is pinned

| Ecosystem | Manifest | Lock or pin | Held by | Reasoning in |
|---|---|---|---|---|
| Cargo | `Cargo.toml` | `Cargo.lock` | `sonar.yml`'s `cargo llvm-cov --locked`; nothing else | comments in `Cargo.toml`; CLAUDE.md → Coding Conventions → Dependencies |
| Rust toolchain | `rust-toolchain.toml` | the exact `channel` | not updated by Dependabot | CLAUDE.md → Rust |
| GitHub Actions | `.github/workflows/*.yml` | the commit SHA after `@`, with the version as comment | review only | CLAUDE.md → Commit Style and Branching |
| cargo-dist | `dist-workspace.toml` (`cargo-dist-version`) | the version in `release.yml` | `dist` itself; `release.yml` is generated and excluded in `.github/dependabot.yml` | the header of `release.yml` |

Not every workflow follows the SHA rule: `release-plz.yml` and
`release-plz-pr.yml` use `release-plz/action@v0.5` and
`dtolnay/rust-toolchain@stable`, and `release.yml` uses tags. Report this
where an update touches them; do not fix it in a Dependabot branch.

Couplings, all inside `Cargo.toml`:

- `polars_excel_writer` builds on a specific `polars` and `rust_xlsxwriter`.
  Ours must resolve to the same versions, or the tree carries two copies and
  the types do not match. `cargo tree -d` shows duplicates;
  `cargo tree -i rust_xlsxwriter` and `cargo tree -i polars` show who wants
  which.
- `rust_xlsxwriter` enables its `polars` feature, so it couples to `polars`
  too.
- `strum` and `strum_macros` move together.
- The `timezones` feature of `polars` is a workaround; its comment says for
  which bug.
- A crate whose `rust-version` exceeds the `channel` in
  `rust-toolchain.toml` fails CI. Raising the toolchain is a change of its
  own (stop and ask).

## Dependabot pull requests

1. Fetch the branches and `main` over HTTPS; `origin` is an SSH remote the
   sandbox cannot reach:
   `git fetch https://github.com/jococo-ch/lola-sumup.git '+refs/heads/dependabot/*:refs/remotes/origin/dependabot/*' '+refs/heads/main:refs/remotes/origin/main'`
2. List them: `gh pr list --repo jococo-ch/lola-sumup --state open`, keeping
   the heads that start with `dependabot/`.
3. Per PR, read:
   - the body, which carries release notes, changelog and commits:
     `gh pr view <n> --repo jococo-ch/lola-sumup --json title,body`;
   - CI, through the workflow runs (`verify.yml`, `sonar.yml`, and the
     `plan` job of `release.yml`, which runs on every PR):
     `gh run list --repo jococo-ch/lola-sumup --branch <head>`, and for a
     failure `gh run view <id> --repo jococo-ch/lola-sumup --log-failed`.
     Read why a run failed before calling it the code's fault: a Dependabot
     PR runs with reduced permissions, which `sonar.yml`'s OIDC auth may not
     get;
   - the diff: `git diff origin/main...origin/<head>`.
4. Check out the head and merge `origin/main` as it stands:
   `git checkout -b <head> origin/<head> && git merge origin/main`.
   Dependabot branched off an older `main`, and every local check runs on
   this merge. A conflict is a finding; abort the merge and report.
5. Assess it along [assessment.md](assessment.md).
6. If the branch needs a commit to pass CI, make it (next section).
7. Report per [assessment.md](assessment.md)'s template.
8. After the last PR, the release recommendation for the whole batch, per
   [assessment.md](assessment.md)'s "The batch: release".

`gh` answering `401 Bad credentials` means the sandbox has no GitHub token.
Public reads (PRs, runs, releases) work unauthenticated through
`curl -s https://api.github.com/repos/jococo-ch/lola-sumup/...`, at a low
rate limit; the security alerts do not. Tell the developer to run on the host
`sbx secret set github --sandbox <$SANDBOX_NAME> -t "$(gh auth token)"`.

## Committing on a Dependabot branch

- Only on the local branch of the PR's own head from step 4. Never on
  `main` or any other branch.
- Only the merge of `origin/main` and what CI needs for this bump: aligning
  a coupled crate from the list above in `Cargo.toml` and `Cargo.lock`, or
  adapting `src/` to an API the bump changed. An adaptation keeps every
  existing test and its input unchanged (CLAUDE.md → TDD); a test that has to
  change to pass is a finding, stop and report.
- Read `git diff` before committing. A `Cargo.lock` change for crates the
  bump did not move is a finding; stop and report.
- Message: `chore(deps): <what> for <dependency> <version>`, ending with the
  attribution line the session prescribes. PRs land squashed (main's
  commits end in `(#n)`), so the PR title, not this message, decides whether
  release-plz releases.
- Never push; the developer does. Say in the report that Dependabot no
  longer rebases a branch someone else has committed to.

## Checks

Free, always, on the merged branch: `just verify` (fmt, clippy, test, as
`verify.yml` runs it) and `cargo build --locked`, which fails where
`sonar.yml`'s `--locked` would. Then `cargo tree -d` for duplicate versions.
The first build compiles polars and takes minutes; use long timeouts.

If `rustc` reports `can't find crate for ...`, `target/` holds stale
artefacts; re-run, then `cargo clean`.

Coverage and the Sonar analysis run in CI only.

## Network and downloads

- Read-only, and only facts about third-party code: changelogs, advisories,
  release metadata, published crates. Never send repository content, and
  never data from `workdir/` or any `*.csv`/`*.xlsx` outside `samples/`.
- Reachable: `github.com`, `api.github.com`, `index.crates.io`,
  `static.crates.io`, `api.osv.dev` (its records carry the RustSec ids), and
  `crates.io/api/v1/...` with a generic `User-Agent` header (without one it
  answers 403). `docs.rs` and `rustsec.org` need approval; the RustSec
  advisories are readable on `github.com/rustsec/advisory-db` instead. A
  blocked host: report host and port, do not work around it.
- A downloaded crate goes into its own new directory from `mktemp -d`,
  outside the repository. Inspect it (`tar tzf`, reading sources); never run
  or build it. A crate in `Cargo.lock` is already under
  `~/.cargo/registry/src/` after a build.

## Stop and ask before

- Any change beyond the PR's own: a toolchain bump, a new feature flag, a
  refactoring the bump invites, a workflow change.
- Running the binary on real data from `workdir/` to compare outputs of old
  and new version. If agreed: on copies in `/tmp`, reporting counts and
  aggregates, never rows (CLAUDE.md → Hard Rules).
- A commit that is not on a `dependabot/*` branch.
- Changing a PR's title; it decides the release.

## Never

- Push, merge, or open a PR.
- Put a secret, key or token into any file or command line.
- Change a test's input, a tolerance or a heuristic boundary to make a bump
  pass. A red test is a finding.
- Edit `CHANGELOG.md`, the version, or `release.yml` by hand.

## Advisories

There is no advisory check in CI (no `cargo audit`, no `cargo deny`), and
neither tool is installed here. The sources are:

- Dependabot security alerts:
  `gh api repos/jococo-ch/lola-sumup/dependabot/alerts` (needs the token);
  their fixes arrive as security update PRs.
- `api.osv.dev` per crate and version, for the whole `Cargo.lock`:
  `POST https://api.osv.dev/v1/querybatch` with the lock's name/version pairs.
