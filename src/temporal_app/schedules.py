"""Temporal Schedule setup for KACE.

Currently registers a single recurring schedule: the federated QLever index
build (`QLeverIndexWorkflow`). The schedule is upserted idempotently on worker
startup so it is always present after a deploy and config changes (cron /
timezone / enabled) propagate without a manual step.

The fired workflow uses the same deterministic id as the manual
`/trigger_qlever_index` endpoint (`qlever-index-build`), and the schedule's
overlap policy is SKIP — so a scheduled fire never stacks on top of an
in-flight build, preserving the at-most-one-in-flight contract.
"""
import logging

from temporalio.client import (
    Client,
    Schedule,
    ScheduleActionStartWorkflow,
    ScheduleAlreadyRunningError,
    ScheduleOverlapPolicy,
    SchedulePolicy,
    ScheduleSpec,
    ScheduleState,
    ScheduleUpdate,
    ScheduleUpdateInput,
)

from config import config

logger = logging.getLogger(__name__)

TASK_QUEUE = "frink-temporal-queue"
# Matches the manual endpoint id so scheduled + manual runs share single-flight.
WORKFLOW_ID = "qlever-index-build"


def _build_schedule() -> Schedule:
    """Render the desired Schedule from current config."""
    return Schedule(
        action=ScheduleActionStartWorkflow(
            "QLeverIndexWorkflow",
            # only_kg=None → full federated rebuild.
            args=[None],
            id=WORKFLOW_ID,
            task_queue=TASK_QUEUE,
        ),
        spec=ScheduleSpec(
            cron_expressions=[config.qlever_index_schedule_cron],
            time_zone_name=config.qlever_index_schedule_timezone,
        ),
        # SKIP: never start a scheduled build while one is still running.
        policy=SchedulePolicy(overlap=ScheduleOverlapPolicy.SKIP),
        state=ScheduleState(
            note="KACE federated QLever index build (weekly).",
            paused=not config.qlever_index_schedule_enabled,
        ),
    )


async def ensure_qlever_index_schedule(client: Client) -> None:
    """Idempotently create-or-update the weekly QLever index build schedule.

    Safe to call on every worker boot. If the schedule already exists, its
    action/spec/policy/state are overwritten so config edits take effect.
    """
    schedule_id = config.qlever_index_schedule_id
    desired = _build_schedule()

    try:
        await client.create_schedule(schedule_id, desired)
        logger.info(
            "Created schedule %s (cron=%r tz=%s enabled=%s)",
            schedule_id,
            config.qlever_index_schedule_cron,
            config.qlever_index_schedule_timezone,
            config.qlever_index_schedule_enabled,
        )
        return
    except ScheduleAlreadyRunningError:
        pass

    # Already exists — reconcile to desired state.
    handle = client.get_schedule_handle(schedule_id)

    async def _updater(_input: ScheduleUpdateInput) -> ScheduleUpdate:
        return ScheduleUpdate(schedule=desired)

    await handle.update(_updater)
    logger.info(
        "Updated existing schedule %s (cron=%r tz=%s enabled=%s)",
        schedule_id,
        config.qlever_index_schedule_cron,
        config.qlever_index_schedule_timezone,
        config.qlever_index_schedule_enabled,
    )
