# Assessing one dependency update

Read by `SKILL.md` for every update, whatever its source. Mark every claim
**read** (taken from a changelog, an advisory, the code or a log),
**measured** (from a command run here) or **inferred**. Where a question
cannot be answered from what is reachable, say so; a guess is not an
assessment.

## What we expose

lola-sumup is a CLI run locally by the treasurer on macOS or Linux. It reads
SumUp's CSV reports and an intermediate xlsx that people edited by hand, and
writes xlsx files that go into the bookkeeping. It opens no network
connection and runs no server. A CVE matters if it is reachable from parsing
those inputs (CSV, xlsx, zip, XML), from writing the outputs, or from the CLI
arguments; code that is linked but never called (e.g. `polars`' cloud stack:
`object_store`, `reqwest`, `rustls`) is "not affected" only with the evidence
that no enabled feature or call of ours reaches it.

The worst outcome of a bump is not a crash but a wrong number in an export
that nobody notices (CLAUDE.md → What to Do When Unsure).

## What to establish

1. **Where it lands.** Shipped binary, build only (proc macros, build
   scripts) or test only (`[dev-dependencies]`). For a crate
   `cargo tree -e normal -i <name>`, per release target where it may differ
   (`--target` with the `targets` of `dist-workspace.toml`). For an action:
   which workflow, and whether it builds the release (`release.yml`,
   `release-plz*.yml`) or only checks (`verify.yml`, `sonar.yml`).
2. **Changelog.** Read the release notes between the two versions: the PR
   body first, then the release pages or `CHANGELOG.md` it links.
3. **Fixed CVEs.** List every advisory the new version fixes, from the
   changelog and from `api.osv.dev` for both versions.
4. **Per CVE, are we affected.** Name the vulnerable function, feature or
   configuration, and search our code for its use, including the Cargo
   features `Cargo.toml` enables and those the tree enables for us
   (`cargo tree -e features -i <name>`). Weigh it against "What we expose".
   Affected, not affected, or cannot tell, with the evidence.
5. **Fixed bugs that concern us.** For each fix, search `src/`, the comments
   in `Cargo.toml`, `README.md` and `git log` for the dependency's name and
   the symptom. Two outcomes matter: a workaround here the fix makes obsolete
   (the `timezones` feature of `polars` is one), and a bug we are exposed to
   without having noticed. A workaround that can go is a proposal, not a
   commit.
6. **New features.** Does one replace something we built ourselves (e.g. a
   helper in `export_details.rs`, a conversion in `export.rs`), or make
   possible something that was not easy before? Name the code it concerns.
7. **Incompatibilities.** Read the `Cargo.lock` diff for anything moving
   beside the bump. Check the couplings `SKILL.md` names: `polars`,
   `polars_excel_writer` and `rust_xlsxwriter` on the same versions,
   `strum` with `strum_macros`, the crate's `rust-version` against
   `rust-toolchain.toml`. For `polars`, `calamine`, `rust_xlsxwriter` and
   `polars_excel_writer` look for changes in behaviour, not only in API:
   rounding, null and NaN handling, datetime and timezone parsing, cell types
   read from or written to xlsx, column naming. They decide the file formats
   (CLAUDE.md → Dependencies).
8. **Regression risk.** The kind of update (patch, minor, major; below 1.0 a
   minor is a major), how much of our code relies on the dependency, and
   whether a test exercises that path. The tests compare data frames
   (`test_utils::assert_dataframe`); writing and reading the xlsx files is
   covered far less, so a writer or reader change is weakly tested. Name
   where a comparison run on real data would be the only evidence, and
   propose it (`SKILL.md` → Stop and ask before).
9. **Supply chain**, as far as it can be judged: age of the release
   (`.github/dependabot.yml` sets no `cooldown`, so a release can be hours
   old), yanked versions, owners or publisher that changed
   (`crates.io/api/v1/crates/<name>/owners` and the version's
   `published_by`), new transitive crates in the lock diff, a new `build.rs`
   or proc macro. For an action, whether the tag in the trailing comment
   points at the pinned SHA
   (`gh api repos/<owner>/<repo>/commits/<tag> -q .sha`), and whether the
   action gets a token or secret in the workflow that uses it (`SECRETS.adoc`
   lists them; `release*.yml` hold write tokens). Say plainly where the
   signal is weak.
10. **What CI needs.** An aligned coupled crate, an adaptation of `src/` to a
    changed API, or a toolchain bump (not made here). Read the PR's CI run
    before deciding.
11. **Recommendation.** Merge, merge after the commit made here, hold, or
    close, with the reason. For a fixed CVE that point 4 found affecting us,
    say that the PR title must carry a releasing type (see the batch).

For a security alert without a PR, points 2 and 6 fall away. Ask instead
whether `cargo update -p <name>` within the current requirement already
brings the fix, or whether a direct dependency has to move first
(`cargo tree -i <name>` shows which).

## The batch: release

After the last update of a request, one recommendation for the whole set,
merged or not: what reaches the users, how, how soon, and how sure that is.

1. **What reaches the released binary**, measured where free: a crate
   `cargo tree -e normal -i <name>` reaches, for the three targets of
   `dist-workspace.toml`. Dev-dependencies ship nothing. An action ships
   nothing, but one in `release.yml` or `release-plz*.yml` touches how the
   release is built and published.
2. **How it ships**: merging the PR to `main`; `release-plz-pr.yml` opens a
   release PR only if `main` has a commit matching `release_commits` in
   `release-plz.toml` since the last release; merging that PR tags; the tag
   runs `release.yml`, which publishes the GitHub release and the Homebrew
   formula in `jococo-ch/homebrew-tap`; users get it through `brew upgrade`
   or the installed updater. A `chore(deps)` title releases nothing on its
   own and stays out of `CHANGELOG.md`. Steps are named, never run.
3. **How soon.** Now only for a fixed advisory that point 4 found affecting
   us: then the PR title needs a releasing type, `fix(deps): ...`, and the
   developer decides. `sec` is a releasing type too, but
   `.github/semantic.yml` lists no types, so the title check may only accept
   the app's defaults, which do not include `sec`. Otherwise with the next
   release a `feat` or `fix` triggers; name what is already waiting on
   `main` since the last tag (`git log <last tag>..origin/main --oneline`).
4. **Confidence**, high, medium or low, resting on the read, measured and
   inferred marks of the statements it uses, and what would raise it. Which
   version the users run is not visible here, so the recommendation is
   relative to `origin/main` and the last release tag.

## Report template

One per update, in the chat, ready for the developer to paste into the PR.
Emit each as raw markdown inside its own fenced block (four backticks, so
nothing in it closes the fence): the chat renders bare markdown, and
rendered text loses its formatting when copied.

```markdown
## Assessment: <dependency> <old> → <new>

### Recommendation

<merge / merge after fix commit / hold / close>: <one sentence>

### Lands in

<shipped binary / build / test / CI workflow>

### Changelog

<link>; <what changed, in two or three lines>

### CVEs fixed

<id: affected / not affected / cannot tell, and why> or none

### Bugs fixed that concern us

<bug: workaround to remove / exposure> or none

### Features of use

<feature: what it replaces or enables> or none

### Incompatibilities

<what> or none found

### Regression risk

<low / medium / high>: <why>

### Supply chain

<signals, and how weak they are>

### CI

<state of the PR's runs; commit made on the branch, if any>

### Not verified

<what this assessment could not check>
```

And once per request, after the last update:

```markdown
## Release: <the updates of this batch>

### Recommendation

<release now / with the next release / nothing to release>: <one sentence>

### What ships

| Update | In the binary | PR title type | Evidence |
|---|---|---|---|
| <dependency> <old> → <new> | <yes / no / build pipeline> | <chore(deps) / fix(deps)> | <measured / read / inferred: how> |

### Why

<the urgency, and what else the next release would carry>

### Confidence

<high / medium / low>: <what it rests on, and what would raise it>
```
