"""Put the pipeline code back to the `baseline` tag and remove the agent's branch.

    uv run python scripts/reset_code.py

- Restores flows/, config/, data/ and tests/ from the `baseline` tag as a NEW
  commit on main (history is never rewritten; no force-push).
- Deletes the remote `autofix/candidate` branch, which also closes its PR.
- Sets the ops Variable back to "ok".
"""

from __future__ import annotations

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))
from _git import PIPELINE_PATHS, git, push_main, sync_main  # noqa: E402

BRANCH = "autofix/candidate"


def main() -> None:
    sync_main()
    git("checkout", "baseline", "--", *PIPELINE_PATHS)
    if git("status", "--porcelain", "--", *PIPELINE_PATHS):
        git("add", "-A", *PIPELINE_PATHS)
        git("commit", "-m", "demo: reset pipeline code to the clean baseline")
        push_main()
        print("main reset to clean baseline:", git("rev-parse", "--short", "HEAD"))
    else:
        print("main already matches the clean baseline")

    if git("ls-remote", "--heads", "origin", BRANCH, deploy_key=True):
        git("push", "origin", "--delete", BRANCH, deploy_key=True)
        print(f"Deleted remote branch {BRANCH} (its PR closes automatically)")

    from prefect.variables import Variable

    Variable.set("harborline_invoicing_ops", "ok", overwrite=True)
    print("harborline_invoicing_ops = ok")


if __name__ == "__main__":
    main()
