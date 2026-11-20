"""Create (or replace) the automation that hands failed production runs to the autofix plan.

    uv run python scripts/create_automation.py

Trigger: a run of deployment harborline-carrier-invoicing/production enters Failed or Crashed.
         (Matched on the production deployment's id, so `candidate` runs never trigger it.)
Action:  run harborline-autofix-dispatch/on-failure with the failed run's id and flow name.
"""

from __future__ import annotations

import asyncio

from prefect.automations import Automation
from prefect.client.orchestration import get_client
from prefect.events.actions import RunDeployment
from prefect.events.schemas.automations import EventTrigger, Posture

NAME = "harborline: autofix failed invoicing (production)"


async def main() -> None:
    async with get_client() as client:
        production = await client.read_deployment_by_name("harborline-carrier-invoicing/production")
        dispatcher = await client.read_deployment_by_name("harborline-autofix-dispatch/on-failure")
        for a in await client.read_automations_by_name(NAME):
            await client.delete_automation(a.id)

    automation = Automation(
        name=NAME,
        description="Hands each failed production run of harborline-carrier-invoicing to the harborline-code-autofix plan.",
        trigger=EventTrigger(
            expect={"prefect.flow-run.Failed", "prefect.flow-run.Crashed"},
            match_related={"prefect.resource.id": f"prefect.deployment.{production.id}"},
            posture=Posture.Reactive,
            threshold=1,
        ),
        actions=[
            RunDeployment(
                source="selected",
                deployment_id=dispatcher.id,
                parameters={"flow_run_id": "{{ flow_run.id }}", "flow_name": "{{ flow.name }}"},
            )
        ],
    )
    created = await automation.acreate()
    print(f"Created automation {created.id}: {NAME}")


if __name__ == "__main__":
    asyncio.run(main())
