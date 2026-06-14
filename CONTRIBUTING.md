# Contributing to Ledge

Thanks for your interest. Ledge is early and moving fast — issues, reproductions,
and focused PRs are all welcome.

## Before you start

- For anything non-trivial, **open an issue first** to align on approach. Large
  unsolicited PRs may be hard to merge.
- Check the honest "Status & limitations" section in the README — some "missing"
  things are deliberate scope choices, and some are exactly where help is wanted
  (e.g. fetch `have`-line negotiation, SSH transport).

## Developer Certificate of Origin (DCO)

Contributions are accepted under the **DCO** — no CLA. By signing off, you certify
you wrote the patch or have the right to submit it under the project license (see
https://developercertificate.org). Sign off every commit:

```sh
git commit -s -m "your message"
```

This appends a `Signed-off-by: Your Name <you@example.com>` trailer. Commits
without a sign-off can't be merged.

## Licensing of contributions

Ledge is **source-available** under the Business Source License 1.1 (BSL 1.1),
which converts to Apache-2.0 on the Change Date (see `LICENSE`). By contributing,
you agree your contribution is licensed under the same terms, including the future
conversion to Apache-2.0.

## Development

```sh
# Build + test the whole workspace
cargo test --workspace

# Lint must be clean (CI enforces this)
cargo clippy --workspace --all-targets -- -D warnings
cargo fmt --all --check

# Formal models (requires Java + TLA+ tools; see formal/)
make -C formal check
```

Bar for a mergeable PR:

- **Tests pass** (`cargo test --workspace`) and you've added tests for new behavior.
- **Zero clippy warnings** (`-D warnings`) and `cargo fmt` clean.
- **No `unsafe`** without a written justification.
- Commits are focused and signed off.
- For protocol/storage changes, prefer a test that uses **real `git`** as the
  oracle (see `crates/ledge-object-store/src/git_pack.rs` for the pattern).

## Reporting security issues

Do **not** open a public issue — see `SECURITY.md`.
