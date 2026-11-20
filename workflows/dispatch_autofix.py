"""harborline-autofix-dispatch: start the code-autofix execution plan for one failed run.

A Prefect automation runs this (deployment `on-failure`) whenever a
`production` run of harborline-carrier-invoicing fails. It starts a run of the
`harborline-code-autofix` plan through the Prefect Cloud API, using the failed
run's id as the idempotency key so a failure is never handled twice.
"""

from __future__ import annotations

from prefect import flow, get_run_logger
from prefect.client.orchestration import get_client

PLAN_FLOW = "harborline-code-autofix"


@flow(name="harborline-autofix-dispatch")
async def dispatch_autofix(flow_run_id: str, flow_name: str) -> str:
    log = get_run_logger()
    async with get_client() as client:
        plan_flow = await client.read_flow_by_name(PLAN_FLOW)
        resp = await client._client.post(
            f"/flows/{plan_flow.id}/execution-plan/runs",
            json={
                "parameters": {"flow_run_id": flow_run_id, "flow_name": flow_name},
                "name": f"autofix-{flow_run_id[:8]}",
                "idempotency_key": f"autofix-{flow_run_id}",
            },
        )
        resp.raise_for_status()
        run = resp.json()
    log.info("Started %s run %s for failed run %s", PLAN_FLOW, run["id"], flow_run_id)
    return run["id"]
