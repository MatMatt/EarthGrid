---
description: Run the EarthGrid gate - release build, tests, and a syntax check on the embedded HTML
allowed-tools: Bash
---

Run these in order from `earthgrid-core/` and stop at the first failure. Report raw output, never a summary of it.

1. `export PATH="$HOME/.cargo/bin:$PATH"; cargo build --release` - must end with 0 errors and 0 warnings.
2. `cargo test` - quote the exact `test result:` line, including ignored counts.
3. `ui.html` and `beacon.html` are `include_str!` assets, so cargo cannot see a JavaScript syntax error in them. Extract every `<script>` block from both files and run `node --check` on each.
4. If `Cargo.lock` or `Cargo.toml` changed, show the resolved version diff and confirm it is the intended change.

Finish with: the commit hash (`git rev-parse --short HEAD`), the build time, the test counts, and any failure verbatim. A failure that is described instead of quoted is not a report.
