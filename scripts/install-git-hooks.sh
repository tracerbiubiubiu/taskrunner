#!/bin/sh
# Install hooks only for this worktree; retain the common fallback for siblings.
set -eu
root=$(git rev-parse --show-toplevel)
for hook in pre-commit commit-msg pre-push; do
    if [ ! -x "${root}/.githooks/${hook}" ]; then
        echo "[FAIL] missing executable hook: .githooks/${hook}" >&2
        exit 1
    fi
done
git config --local extensions.worktreeConfig true
git config --worktree core.hooksPath .githooks
echo "[OK] hooks installed for this worktree; common core.hooksPath preserved"
