---
description: Build and deploy EarthGrid on nucleus, with the stop-copy-start sequence and verification
allowed-tools: Bash
---

Never copy over a running binary: the copy fails and the service restarts on the old one.

1. `cd ~/dev/EarthGrid` on nucleus and confirm HEAD (`git log --oneline -1`) is the commit you intend to ship.
2. `export PATH="$HOME/.cargo/bin:$PATH"; cd earthgrid-core; cargo build --release`
3. `cp -a ~/.cargo/bin/earthgrid ~/.cargo/bin/earthgrid.bak-<short-sha>`
4. `export XDG_RUNTIME_DIR=/run/user/$(id -u); systemctl --user stop earthgrid; sleep 2; cp target/release/earthgrid ~/.cargo/bin/earthgrid; systemctl --user start earthgrid`
5. Wait for `ss -tln | grep 8400` before judging anything. A warm restart binds in about 25 s; a cold one replays the WAL and takes 3 to 4 minutes with the main thread in D state. Do not kill it and do not reset anything.
6. Verify, and report each result: `md5sum ~/.cargo/bin/earthgrid` against the build output; `curl localhost:8400/health`; `curl -L localhost:8400/dashboard`; `curl localhost:8400/api/beacon/nodes` still listing the nodes. For a UI change, grep the deployed binary for a string the change introduced, because the assets are embedded.

The Lenovo is a second node and is deployed by hand the same way.
