"""Generate the harborline-code-autofix execution plan.

    uv run python workflows/build_plan.py \
        --production-deployment <uuid> --candidate-deployment <uuid>

Writes workflows/harborline-code-autofix.plan.json. The design (interview log,
2026-11-20) is in the README. Each node gets only the tools it needs.
"""

from __future__ import annotations

import argparse
import json
from pathlib import Path

HERE = Path(__file__).parent

# ---- workspace-specific ids (re-derive per workspace) ------------------------
PREFECT_PROVIDER = "cbb49911-e778-57e6-a1a4-1336c56e081a"  # list_provider_connections -> prefect
OPS_DESK_CONNECTION = "06afcb7c-21a4-7c08-8000-754448c55a71"  # Harborline Ops Desk MCP
GITHUB_CONNECTION = "06b0074d-dcf9-73e7-8000-c79acab63b69"  # GitHub remote MCP (fine-grained PAT)
APP_URL = "https://app.stg.prefect.dev/account/082ab537-848d-465a-a6ba-9da372bfc4ca/workspace/d70b4af3-5132-4ea1-8384-e9d94c896e69"

OWNER, REPO = "forester-maker", "harborline-pipelines"
BRANCH = "autofix/candidate"
OPUS, SONNET, HAIKU = "claude-opus-5-5", "claude-sonnet-5-5", "claude-haiku-5-5"
CHANNEL = "#data-platform-alerts"


# ---- helpers ----------------------------------------------------------------

def model(model_id: str, effort: str | None = None) -> dict:
    m = {"connection_id": PREFECT_PROVIDER, "id": model_id}
    if effort:
        m["effort"] = effort
    return m


def obj(props: dict, required: list[str] | None = None) -> dict:
    return {"type": "object", "properties": props, "required": required if required is not None else list(props)}


S, I, B = {"type": "string"}, {"type": "integer"}, {"type": "boolean"}
STRS = {"type": "array", "items": S}
ANY_OBJ = {"type": "object"}
BLOCKED = {"schema": obj({"reason": S, "summary": S})}


def one(schema: dict) -> dict:
    return {"schema": schema, "expects": {"cardinality": "exactly_one", "shape": "value"}}


def maybe(schema: dict) -> dict:
    return {"schema": schema, "expects": {"cardinality": "zero_or_one", "shape": "value"}}


def orch(selection: str = "exactly_one", when: str = "all_required_inputs_produced") -> dict:
    return {"output_selection": selection, "evaluate_when": when}


def ops(*tools: str) -> dict:
    return {"type": "connection", "connection_id": OPS_DESK_CONNECTION, "tools": list(tools)}


def gh(*tools: str) -> dict:
    return {"type": "connection", "connection_id": GITHUB_CONNECTION, "tools": list(tools)}


def edge(src, dst_node: str, dst_input: str, eid: str | None = None) -> dict:
    if isinstance(src, str):
        frm = {"type": "plan_input", "input": src}
        default_id = f"{src}-to-{dst_node}.{dst_input}"
    else:
        node, out = src
        frm = {"type": "node_output", "node": node, "output": out}
        default_id = f"{node}.{out}-to-{dst_node}.{dst_input}"
    return {"id": eid or default_id, "from": frm, "to": {"node": dst_node, "input": dst_input}}


# ---- schemas ----------------------------------------------------------------

FAILURE = obj(
    {
        "flow_run_id": S, "flow_run_name": S, "flow_name": S, "deployment": S,
        "failed_task": S, "exception_type": S, "error": S,
        "traceback_frames": STRS, "key_log_lines": STRS, "summary": S,
    }
)
CODE = obj(
    {
        "main_head_sha": S,
        "files": {"type": "array", "items": obj({"path": S, "content": S})},
        "recent_commits": {"type": "array", "items": obj({"sha": S, "message": S, "files_changed": STRS, "patch_excerpt": S})},
        "summary": S,
    }
)
DIAGNOSIS = obj(
    {
        "flow_run_id": S, "error": S, "root_cause": S, "file": S, "line": I,
        "offending_commit_sha": S, "offending_commit_message": S,
        "fix_location": {"type": "string", "enum": ["flows", "outside_flows"]},
        "intended_fix": S, "covering_test": S, "main_head_sha": S, "summary": S,
    }
)
NOT_CODE = obj({"flow_run_id": S, "failure_class": S, "error": S, "reason": S, "summary": S})
FIX = obj(
    {
        "branch": S, "base_sha": S, "commit_sha": S, "files_changed": STRS, "lines_changed": I,
        "commit_message": S, "change_summary": S, "summary": S,
    }
)
OUTSIDE = obj({"reason": S, "needed_change": S, "file": S, "summary": S})
# A DeploymentNode only accepts `parameters`, so the outputs that start the candidate and
# production runs ARE the flow's parameters (with provenance about what is being tested).
RUN_PARAMS = {
    "type": "object",
    "properties": {"month": S, "triggered_by": S, "source_commit": S},
    "required": ["month", "triggered_by", "source_commit"],
    "additionalProperties": False,
}
CHECK_PASSED = RUN_PARAMS
CHECK_FAILED = obj({"violations": STRS, "summary": S})
PR = obj({"pr_number": I, "pr_url": S, "title": S, "candidate_run_url": S, "summary": S})
REVIEW = obj({"decision": S, "notes": S}, ["decision"])
MERGED = RUN_PARAMS
REPORTED = obj(
    {
        "ending": {
            "type": "string",
            "enum": [
                "fixed_and_deployed", "fixed_but_production_failed", "candidate_failed",
                "fix_rejected_by_check", "fix_outside_allowed_paths", "rejected_by_reviewer",
                "awaiting_review", "not_a_code_bug", "blocked",
            ],
        },
        "message": S,
        "summary": S,
    }
)

WRITE_RULES = (
    "Rules for the fix (hard limits; the next node checks every one): "
    f"(1) commit ONLY to branch `{BRANCH}`, never to `main`; "
    "(2) change ONLY files under `flows/`; never `tests/`, `config/`, `data/`, `prefect.yaml`, `scripts/`, `workflows/` or `.github/`; "
    "(3) at most 20 changed lines in total (additions + deletions); "
    "(4) exactly one commit; "
    "(5) never remove or weaken a validation (`raise`, `assert`, or an `if … raise` check) — a value that fails "
    "validation is a data/config problem, not a code bug; "
    "(6) fix forward with the smallest change; never revert the offending commit."
)


# ---- the plan -----------------------------------------------------------------

def build(production_deployment: str, candidate_deployment: str) -> dict:
    repo = f"`{OWNER}/{REPO}`"
    nodes = {
        "inspect_failure": {
            "kind": "AgentNode",
            "label": "Read the failed production run",
            "objective": (
                "A production run of the Harborline carrier-invoicing pipeline failed; its id is in `flow_run_id`. "
                "Call `get_flow_run_failure` once with that id, `include_traceback: true` and `max_log_lines: 20`. "
                "Copy into `failure_read`: the run id, run name, flow name, deployment, the failed task (first task in "
                "state Failed, numeric suffix removed), the exception type, the error message quoted exactly, the "
                "`traceback_frames` exactly as returned, and up to 5 key ERROR/WARNING log lines quoted exactly. "
                "Set `summary` to one sentence naming the task and the error. "
                "If the run can't be read, isn't Failed/Crashed, or isn't a production run, select `blocked` with the reason."
            ),
            "model": model(HAIKU, "low"),
            "mcp": {"mcpServers": {"ops": ops("get_flow_run_failure")}},
            "inputs": {"flow_run_id": one(S)},
            "outputs": {"failure_read": {"schema": FAILURE}, "blocked": BLOCKED},
            "orchestration": orch(),
        },
        "read_code": {
            "kind": "AgentNode",
            "label": "Read the pipeline code and recent changes",
            "objective": (
                f"Read the pipeline's code on `main` of {repo} so a later step can diagnose a failure in flow `flow_name`. "
                "With `get_file_contents` (owner, repo, ref `main`) read exactly these files: `flows/invoicing.py`, "
                "`config/rates.yaml`, `tests/test_invoicing.py`, and `data/scans.csv` (keep only its first 6 lines). "
                "With `list_commits` (sha `main`, perPage 5) get the latest commits; set `main_head_sha` to the newest. "
                "For each of the 3 newest commits call `get_commit` and record sha, message, changed file paths, and a "
                "patch excerpt of at most 40 lines. Copy file contents exactly. Do not diagnose. "
                "Select `code_read` with `summary` naming the newest commit; select `blocked` if the repo can't be read."
            ),
            "model": model(SONNET, "low"),
            "mcp": {"mcpServers": {"github": gh("get_file_contents", "list_commits", "get_commit")}},
            "inputs": {"flow_name": one(S)},
            "outputs": {"code_read": {"schema": CODE}, "blocked": BLOCKED},
            "orchestration": orch(),
        },
        "diagnose": {
            "kind": "AgentNode",
            "label": "Diagnose: is it a code bug, and where?",
            "objective": (
                "Decide whether the failure in `failure` is caused by the code or files in `code`, using only these two "
                "inputs. Match the error and traceback to the file and line, then look through `code.recent_commits` for "
                "the commit that introduced the cause. "
                "Select `not_a_code_bug` when the cause is operational (timeouts, connection or credential errors, "
                "infrastructure, an outage toggle) and no recent commit explains it; say which failure class it looks like. "
                "Otherwise select `code_bug`: give the root cause in 1-2 sentences, the file and line, the offending commit "
                "sha and message, `fix_location` (`flows` when the correct fix is a code change under `flows/`; "
                "`outside_flows` when the correct fix is in config, data or tests — for example a value that a validation "
                "rightly rejects), the intended fix as one or two sentences, the test in `tests/` that covers the behavior "
                "(or 'none'), and `main_head_sha` from `code`. Copy the error from `failure` exactly. "
                "Select `blocked` if the inputs are empty or contradictory. Set `summary` to one sentence."
            ),
            "model": model(OPUS),
            "inputs": {"failure": one(FAILURE), "code": one(CODE)},
            "outputs": {"code_bug": {"schema": DIAGNOSIS}, "not_a_code_bug": {"schema": NOT_CODE}, "blocked": BLOCKED},
            "orchestration": orch(),
        },
        "write_fix": {
            "kind": "AgentNode",
            "label": "Write the fix on autofix/candidate",
            "objective": (
                f"Fix the bug described in `diagnosis` in {repo}. "
                "If `diagnosis.fix_location` is `outside_flows`, or the only correct fix would break any rule below, change "
                "nothing and select `fix_outside_allowed_paths` with the reason and the change a person should make. "
                f"Otherwise: (a) call `create_branch` for `{BRANCH}` from `main`; if it already exists, select `blocked` with "
                "'autofix/candidate already exists; run scripts/reset_code.py'. (b) Read the file with `get_file_contents` "
                f"(ref `{BRANCH}`). (c) Write the minimal fix. Before pushing, write and run a short Python script that diffs "
                "old vs new content with difflib and counts changed lines; it must be 1–20. (d) Push it with ONE `push_files` "
                f"call to branch `{BRANCH}`, with commit message `autofix: <one line>` followed by a blank line, "
                "'Fixes failure in run <flow_run_id>', and 'Introduced by <offending_commit_sha>'. "
                "Select `fix_committed` with the branch, base sha (`diagnosis.main_head_sha`), the new commit sha, files "
                "changed, the scripted line count, the commit message, and a 1–2 sentence change summary. "
                + WRITE_RULES
            ),
            "model": model(OPUS),
            "mcp": {"mcpServers": {"github": gh("create_branch", "get_file_contents", "push_files")}},
            "inputs": {"diagnosis": one(DIAGNOSIS)},
            "outputs": {
                "fix_committed": {"schema": FIX},
                "fix_outside_allowed_paths": {"schema": OUTSIDE},
                "blocked": BLOCKED,
            },
            "orchestration": orch(),
        },
        "check_fix": {
            "kind": "AgentNode",
            "label": "Independent review of the diff (read-only)",
            "objective": (
                f"Independently check the commit described in `fix` on {repo} against the rules below. Do not trust `fix`; "
                f"read the source. Call `list_commits` with sha `{BRANCH}` (perPage 3): the newest commit must equal "
                "`fix.commit_sha` and the one before it must equal `fix.base_sha` (exactly one new commit). Call `get_commit` "
                "for `fix.commit_sha` and write and run a short script over its file list and patch that checks: the diff is "
                "not empty; every changed path starts with `flows/`; additions + deletions ≤ 20; no removed line contains "
                "`raise` or `assert`. Select `passed` only if every check passes. Its value is exactly the parameters for the "
                "candidate run, and nothing else: `month` = '2026-10', `triggered_by` = 'autofix candidate for run "
                "<flow_run_id>', `source_commit` = `fix.commit_sha`. Otherwise select `failed`, listing each violation with "
                "the numbers you measured. An empty or unreadable diff is a failure. "
                + WRITE_RULES
            ),
            "model": model(SONNET, "low"),
            "mcp": {"mcpServers": {"github": gh("list_commits", "get_commit")}},
            "inputs": {"fix": one(FIX), "flow_run_id": one(S)},
            "outputs": {"passed": {"schema": CHECK_PASSED}, "failed": {"schema": CHECK_FAILED}},
            "orchestration": orch(),
        },
        "verify": {
            "kind": "DeploymentNode",
            "label": "Run the candidate deployment (pulls autofix/candidate)",
            "deployment": {"id": candidate_deployment},
            "wait": {"for": "child_flow_run", "until": "terminal"},
            "inputs": {"parameters": one(CHECK_PASSED)},
            "outputs": {"completed": {"schema": ANY_OBJ}, "failed": {"schema": ANY_OBJ}},
            "orchestration": orch(),
        },
        "open_pr": {
            "kind": "AgentNode",
            "label": "Open the pull request",
            "objective": (
                f"The fix in `fix` passed review and the candidate run in `verification` completed. Open a pull request on "
                f"{repo} from `{BRANCH}` into `main` with `create_pull_request`. Title: `autofix: <short root cause>`. "
                "Body, in markdown, under these headings: **What failed** (the error, quoted, and the production run link "
                f"{APP_URL}/runs/flow-run/<diagnosis.flow_run_id>); **Root cause** (from `diagnosis`, naming the offending "
                "commit sha and message); **Fix** (`fix.change_summary`, files and line count); **Proof** (the candidate run "
                f"link {APP_URL}/runs/flow-run/<the run id found in `verification`>, state Completed); and the line "
                "'Opened by the harborline-code-autofix agentic workflow. Merging needs human approval.' "
                "Select `pr_opened` with the PR number, URL, title and the candidate run URL; select `blocked` on error."
            ),
            "model": model(HAIKU, "low"),
            "mcp": {"mcpServers": {"github": gh("create_pull_request")}},
            "inputs": {"diagnosis": one(DIAGNOSIS), "fix": one(FIX), "verification": one(ANY_OBJ)},
            "outputs": {"pr_opened": {"schema": PR}, "blocked": BLOCKED},
            "orchestration": orch(),
        },
        "approve_merge": {
            "kind": "HumanInputNode",
            "label": "Reviewer: merge the agent's fix?",
            "human_input": {
                "form_schema": {
                    "type": "object",
                    "title": "Merge the autofix pull request?",
                    "description": (
                        "The agent diagnosed a code bug, committed a fix to autofix/candidate, an independent check reviewed "
                        "the diff, and the candidate run passed. Review the PR, then approve to merge it and run production, "
                        "or reject with notes (the PR stays open for a developer). If nobody answers within 30 minutes, "
                        "nothing is merged."
                    ),
                    "properties": {
                        "decision": {"type": "string", "enum": ["approve_merge", "reject"], "title": "Decision"},
                        "notes": {"type": "string", "title": "Notes", "description": "Required when rejecting; added to the PR."},
                    },
                    "required": ["decision"],
                },
                "deadline": {
                    "after": "PT30M",
                    "on_expiry": {
                        "output": "no_answer",
                        "value": {"decision": "no_answer", "notes": "No answer within 30 minutes; the PR is left open and not merged."},
                    },
                },
            },
            "inputs": {"pr": one(PR), "diagnosis": one(DIAGNOSIS), "fix": one(FIX)},
            "outputs": {"approve_merge": {"schema": REVIEW}, "reject": {"schema": REVIEW}, "no_answer": {"schema": REVIEW}},
            "orchestration": orch(),
        },
        "merge_pr": {
            "kind": "AgentNode",
            "label": "Merge the approved PR",
            "objective": (
                f"The reviewer approved. Merge pull request `pr.pr_number` on {repo} with `merge_pull_request`, method "
                "`squash`, keeping the PR title as the commit title. Do nothing else. Select `merged`; its value is exactly "
                "the parameters for the production run, and nothing else: `month` = '2026-10', `triggered_by` = 'autofix "
                "merge of PR #<pr_number>', `source_commit` = the merge commit sha. Select `blocked` with the error if the "
                "merge fails."
            ),
            "model": model(HAIKU, "low"),
            "mcp": {"mcpServers": {"github": gh("merge_pull_request")}},
            "inputs": {"pr": one(PR), "review": one(REVIEW)},
            "outputs": {"merged": {"schema": MERGED}, "blocked": BLOCKED},
            "orchestration": orch(),
        },
        "run_production": {
            "kind": "DeploymentNode",
            "label": "Run production on the merged main",
            "deployment": {"id": production_deployment},
            "wait": {"for": "child_flow_run", "until": "terminal"},
            "inputs": {"parameters": one(MERGED)},
            "outputs": {"completed": {"schema": ANY_OBJ}, "failed": {"schema": ANY_OBJ}},
            "orchestration": orch(),
        },
        "report": {
            "kind": "AgentNode",
            "label": "Report the outcome to the team",
            "objective": (
                "Post exactly one message about how this autofix run ended. `outcome` maps the edge that fired to its "
                "value; the edge id names the ending (for example `ending.run_production.completed`). Map it to `ending`: "
                "run_production.completed → fixed_and_deployed; run_production.failed → fixed_but_production_failed; "
                "verify.failed → candidate_failed; check_fix.failed → fix_rejected_by_check; "
                "write_fix.fix_outside_allowed_paths → fix_outside_allowed_paths; approve_merge.reject → "
                "rejected_by_reviewer; approve_merge.no_answer → awaiting_review; diagnose.not_a_code_bug → not_a_code_bug; "
                "any `.blocked` → blocked. "
                f"Call `post_message` to `{CHANNEL}` with at most 6 lines: the ending in plain words, the failed run "
                f"({APP_URL}/runs/flow-run/<flow_run_id>), the root cause or failure class, and the fix commit, PR link, and "
                "candidate/production run links where they exist. For fixed_but_production_failed start with "
                "'HIGH PRIORITY: production still failing after merge' and name the merge commit (`merged.source_commit`). For rejected_by_reviewer "
                "also call `add_issue_comment` on the PR (issue_number = PR number) with the reviewer's notes. For "
                "awaiting_review say 'fix proven, PR waiting for review'. Never claim a fix that didn't happen. "
                "Select `reported` with the ending, the message text, and a one-sentence summary."
            ),
            "model": model(HAIKU, "low"),
            "mcp": {"mcpServers": {"ops": ops("post_message"), "github": gh("add_issue_comment")}},
            "inputs": {
                "outcome": {
                    "schema": ANY_OBJ,
                    "expects": {"cardinality": "one_or_more", "shape": "source_map", "key_by": "edge"},
                },
                "flow_run_id": one(S),
                "diagnosis": maybe(DIAGNOSIS),
                "fix": maybe(FIX),
                "pr": maybe(PR),
                "merged": maybe(MERGED),
            },
            "outputs": {"reported": {"schema": REPORTED}},
            "orchestration": orch(when="all_reachable_terminal"),
        },
    }

    endings = [
        ("inspect_failure", "blocked"), ("read_code", "blocked"),
        ("diagnose", "not_a_code_bug"), ("diagnose", "blocked"),
        ("write_fix", "fix_outside_allowed_paths"), ("write_fix", "blocked"),
        ("check_fix", "failed"), ("verify", "failed"), ("open_pr", "blocked"),
        ("approve_merge", "reject"), ("approve_merge", "no_answer"),
        ("merge_pr", "blocked"), ("run_production", "completed"), ("run_production", "failed"),
    ]

    edges = [
        edge("flow_run_id", "inspect_failure", "flow_run_id"),
        edge("flow_name", "read_code", "flow_name"),
        edge("flow_run_id", "report", "flow_run_id"),
        edge(("inspect_failure", "failure_read"), "diagnose", "failure"),
        edge(("read_code", "code_read"), "diagnose", "code"),
        edge(("diagnose", "code_bug"), "write_fix", "diagnosis"),
        edge(("write_fix", "fix_committed"), "check_fix", "fix"),
        edge("flow_run_id", "check_fix", "flow_run_id"),
        edge(("check_fix", "passed"), "verify", "parameters"),
        edge(("diagnose", "code_bug"), "open_pr", "diagnosis"),
        edge(("write_fix", "fix_committed"), "open_pr", "fix"),
        edge(("verify", "completed"), "open_pr", "verification"),
        edge(("open_pr", "pr_opened"), "approve_merge", "pr"),
        edge(("diagnose", "code_bug"), "approve_merge", "diagnosis"),
        edge(("write_fix", "fix_committed"), "approve_merge", "fix"),
        edge(("open_pr", "pr_opened"), "merge_pr", "pr"),
        edge(("approve_merge", "approve_merge"), "merge_pr", "review"),
        edge(("merge_pr", "merged"), "run_production", "parameters"),
        edge(("merge_pr", "merged"), "report", "merged"),
        edge(("diagnose", "code_bug"), "report", "diagnosis"),
        edge(("write_fix", "fix_committed"), "report", "fix"),
        edge(("open_pr", "pr_opened"), "report", "pr"),
    ] + [edge((n, o), "report", "outcome", eid=f"ending.{n}.{o}") for n, o in endings]

    outputs = {
        "result": {
            "fields": {"result": {"schema": REPORTED, "source": {"type": "node_output", "node": "report", "output": "reported"}}},
            "required": ["result"],
        },
        "diagnosis": {"fields": {"diagnosis": {"schema": DIAGNOSIS, "source": {"type": "node_output", "node": "diagnose", "output": "code_bug"}}}},
        "pull_request": {"fields": {"pull_request": {"schema": PR, "source": {"type": "node_output", "node": "open_pr", "output": "pr_opened"}}}},
    }

    return {
        "kind": "ExecutionPlan",
        "schema_version": "0.2",
        "inputs": {
            "flow_run_id": {
                "schema": {"type": "string", "title": "Failed flow run id", "description": "UUID of the failed production run.", "minLength": 36},
                "required": True,
            },
            "flow_name": {
                "schema": {"type": "string", "title": "Flow name", "description": "Flow of the failed run, e.g. harborline-carrier-invoicing.", "minLength": 1},
                "required": True,
            },
        },
        "nodes": nodes,
        "edges": edges,
        "outputs": outputs,
    }


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--production-deployment", required=True)
    ap.add_argument("--candidate-deployment", required=True)
    ap.add_argument("--out", default=str(HERE / "harborline-code-autofix.plan.json"))
    args = ap.parse_args()
    plan = build(args.production_deployment, args.candidate_deployment)
    Path(args.out).write_text(json.dumps(plan, indent=2) + "\n")
    print(f"wrote {args.out} ({len(plan['nodes'])} nodes, {len(plan['edges'])} edges)")


if __name__ == "__main__":
    main()
