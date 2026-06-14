## What & why

<!-- What does this change and why. Link the issue it addresses. -->

## Checklist

- [ ] `cargo test --workspace` passes
- [ ] `cargo clippy --workspace --all-targets -- -D warnings` is clean
- [ ] `cargo fmt --all --check` is clean
- [ ] New behavior has tests (protocol/storage changes use real `git` as the oracle where possible)
- [ ] No `unsafe` without written justification
- [ ] Commits are signed off (`git commit -s`) per the DCO in CONTRIBUTING.md
