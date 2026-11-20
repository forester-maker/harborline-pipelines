"""Git helpers for the demo-control scripts.

Pushes to `main` go through the repo's **deploy key**, the only actor allowed to
bypass the `main` ruleset (require a pull request). The agent's GitHub token and
personal logins can't push to `main` directly. They merge through a PR.

Set HARBORLINE_DEPLOY_KEY to override the key path.
"""

from __future__ import annotations

import os
import subprocess
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
DEPLOY_KEY = Path(os.environ.get("HARBORLINE_DEPLOY_KEY", "~/.ssh/harborline_pipelines_deploy")).expanduser()
PIPELINE_PATHS = ["flows", "config", "data", "tests"]


def git(*args: str, check: bool = True, deploy_key: bool = False) -> str:
    env = dict(os.environ)
    if deploy_key:
        if not DEPLOY_KEY.exists():
            raise SystemExit(f"Deploy key not found at {DEPLOY_KEY}. See README: 'Deploy key and ruleset'.")
        # -F /dev/null + IdentityAgent=none: ignore ~/.ssh/config and the agent, which would
        # otherwise offer your personal key first (that key gets no ruleset bypass).
        env["GIT_SSH_COMMAND"] = f"ssh -F /dev/null -i {DEPLOY_KEY} -o IdentitiesOnly=yes -o IdentityAgent=none"
    r = subprocess.run(["git", *args], cwd=ROOT, env=env, text=True, capture_output=True)
    if check and r.returncode != 0:
        raise SystemExit(f"git {' '.join(args)} failed:\n{r.stderr.strip()}")
    return r.stdout.strip()


def sync_main() -> None:
    """Bring local main up to date (the agent merges PRs on GitHub)."""
    if git("status", "--porcelain", "--", *PIPELINE_PATHS):
        raise SystemExit(f"Uncommitted changes under {PIPELINE_PATHS}; commit or discard them first.")
    git("checkout", "main")
    git("pull", "--ff-only", "origin", "main", deploy_key=True)


def push_main() -> None:
    git("push", "origin", "main", deploy_key=True)
