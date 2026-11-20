# Harborline pipelines: a self-healing pipeline demo

A Prefect flow fails because of a **code bug**. A Prefect **agentic workflow** then:
1. reads the failure and the code;
2. diagnoses the bug;
3. writes a fix on a branch;
4. has an independent agent review the diff;
5. **proves** the fix with a Prefect run from that branch;
6. opens a PR.

A person approves the PR. The workflow then merges it and runs production on the new `main`. Code reaches `main` only when a person said yes.

Harborline Logistics is fictional. All data in this repo is made up.

## The pieces

| What | Where |
|---|---|
| The pipeline: `harborline-carrier-invoicing`, monthly invoice totals per carrier | `flows/invoicing.py`, `config/rates.yaml`, `data/scans.csv`, `tests/` |
| Deployments: `production` (pulls `main`), `candidate` (pulls `autofix/candidate`), `on-failure` (dispatcher) | `prefect.yaml` on the Prefect-managed pool `harborline-managed` |
| Trigger: automation on `Failed`/`Crashed` of **production only**, which runs the dispatcher, which starts the plan | `scripts/create_automation.py`, `workflows/dispatch_autofix.py` |
| The agentic workflow `harborline-code-autofix` (11 nodes) | `workflows/build_plan.py` → `workflows/harborline-code-autofix.plan.json` |
| Demo controls | `scripts/inject_bug.py`, `scripts/reset_code.py`, `scripts/break_ops.py` |

```
inspect_failure (Haiku, Ops Desk) ─┐
                                   ├→ diagnose (Opus, no tools) ─┬─ code_bug → write_fix (Opus; the only writer, on autofix/candidate)
read_code (Sonnet, GitHub read) ───┘                             └─ not_a_code_bug ─────────────────────────────────────┐
write_fix → check_fix (Sonnet, read-only diff review) → verify [Deployment: candidate]                                    │
verify ✓ → open_pr (Haiku) → approve_merge [human, 30 min, silence never merges] → merge_pr (Haiku)                       │
       → run_production [Deployment: production] ───────────────────────────────────────→ report (Haiku) ←──────────────┘
                                         (every ending and every `blocked` output also flows into `report`)
```

### Guardrails
- **The `main` ruleset (`protect-main`):** requires a pull request (0 approvals) and blocks force pushes. Its only bypass is the repo **deploy key**, which the demo scripts use. The agent's GitHub token, and your own login, **can't push to `main`**: this was checked, and a direct push is rejected with `GH013`. The agent can only merge a PR after a person approves.
  - *The repo is public:* GitHub's free plan doesn't enforce rulesets on private repos.
- **Rules for `write_fix`:**
  - only `flows/`;
  - at most 20 changed lines;
  - one commit, only on `autofix/candidate`;
  - never weaken validation;
  - fix forward rather than revert.
- **`check_fix`:** an independent, read-only agent re-measures the diff before any code runs.
- **Least privilege:** each node gets only the tools it needs. `diagnose` has none, and only `merge_pr` can merge.

## Before a demo (about 2 minutes)

```bash
uv run python scripts/reset_code.py        # main back to the `baseline` tag (forward commit), delete autofix/candidate, ops toggle ok
```
Then open these tabs: Prefect (the plan's flow page and the production deployment), GitHub (the repo's commits and PRs), and the Ops Desk console if you're showing `#data-platform-alerts`.

## Demo controls

```bash
uv run python scripts/inject_bug.py --list
uv run python scripts/inject_bug.py rate_field        # commit the bug to main, push, start a production run
uv run python scripts/break_ops.py upstream_outage    # operational failure, no code change
uv run python scripts/break_ops.py ok
uv run python scripts/reset_code.py
```

| Bug | What it does | Expected ending |
|---|---|---|
| `rate_field` | carrier ops renames `rate_per_kg` → `rate_per_kilo`, giving `KeyError` | **fixed_and_deployed** (after you approve) |
| `zero_shipments` | adds a carrier with no shipments and removes the divide-by-zero guard, giving `ZeroDivisionError` | fixed, then your decision |
| `date_format` | the feed switches to DD/MM dates, giving `ValueError` on the 13th | fixed, then your decision (a spare live bug) |
| `bad_config` | Ridgeway rate set to `-0.38`; the validation rightly rejects it | **fix_outside_allowed_paths**: no branch, no change |
| `break_ops.py upstream_outage` | warehouse timeout, no code change | **not_a_code_bug**: nothing touched |

## Run of show: the "self-healing pipeline" act (about 12 minutes)

Use it on its own, or as act 3 of the Harborline demo (after pipeline triage).

1. **0:00** `inject_bug.py rate_field`. Show the commit on GitHub: "carrier ops renamed a field upstream".
2. **0:30** The production run fails in Prefect. Show the automation, then the plan run appearing by itself.
3. **1:00–4:00** Walk the graph while it runs:
   - the two reads run in parallel;
   - `diagnose` reads the code with **no tools**, and names the offending commit and the line;
   - `write_fix` is the only node with write access;
   - `check_fix` re-measures the diff independently;
   - the candidate run goes green **on the fix branch**.
4. **~4:30** The approval form appears. Open the **real PR**: the root cause, the offending commit, an 11-line diff, and a link to the passing candidate run. Approve.
5. **~5:00–7:00** The merge, then the production run on the new `main` goes green, then the channel message.
6. **Remaining time.** Show the runs you prepared earlier:
   - `bad_config`: "it refused to weaken a validation; no branch was created";
   - `upstream_outage`: "not a code bug; it never got write access";
   - a **rejected** PR with your notes.

   Point at the ruleset: "even the agent's token can't push to `main`".

Measured on 2026-11-20:
- bug to approval form: **about 4.5 min**;
- approval to production green: **about 2.5 min**;
- not-a-code-bug and guardrail endings: **about 2–3 min**.

## Test results (2026-11-20, staging sandbox)

| | Trigger | Ending | Notes |
|---|---|---|---|
| A | `rate_field`, approve | `fixed_and_deployed` | PR #1, merge `6f9d023`, production completed |
| B | `zero_shipments`, reject | see below | |
| C | `upstream_outage` | `not_a_code_bug` | never reached `write_fix` |
| D | `bad_config` | `fix_outside_allowed_paths` | refused to edit config or weaken validation; no branch |

Untested by design:
- a 30-minute expiry;
- a candidate run that fails;
- production failing after the merge.

## Setup (once)

1. Create the GitHub repo, then add the deploy key `~/.ssh/harborline_pipelines_deploy.pub` with **write access** under repo **Settings → Deploy keys**. Don't add it to your account keys. The scripts call ssh with `-F /dev/null` so your `~/.ssh/config` can't offer your personal key instead.
2. Create the ruleset `protect-main`: active on the default branch; require a PR (0 approvals); block force pushes; restrict deletions; bypass = **Deploy keys**.
3. Create two fine-grained tokens, both scoped to this repo only:
   - **agent:** Contents and Pull requests read/write. It goes on the Prefect MCP connection `GitHub (forester-maker)`.
   - **clone:** Contents read-only. It goes in the Prefect Secret block `harborline-github-read`.
4. `uv run prefect deploy --all`, then `uv run python scripts/create_automation.py`.
5. `uv run python workflows/build_plan.py --production-deployment <id> --candidate-deployment <id>`, then publish with the `prefect-agentic-workflows` MCP.
6. The Ops Desk MCP (Horizon) must allow `harborline-carrier-invoicing` in `DEMO_FLOW_ALLOWLIST`, which is the default from commit `63c2715`.

## Gotchas found while building

- **Pool type:** this sandbox's plan refuses process and hybrid pools ("does not support hybrid or push work pools"). A **managed** pool works. Its runs can't use your SSH key, hence the read-only clone token.
- **Deployment-node inputs:** a plan's Deployment node accepts only `parameters` (and `job_variables`). That's why the flow has the provenance parameters `triggered_by` and `source_commit`, which the upstream nodes fill.
- **Rulesets on free private repos:** they aren't enforced. Make the repo public or use GitHub Pro.
- **The GitHub MCP has no "compare" tool:** `check_fix` reviews the single fix commit with `get_commit`.
- **Resetting:** it's a forward commit, which never rewrites `main` and needs no force-push.
