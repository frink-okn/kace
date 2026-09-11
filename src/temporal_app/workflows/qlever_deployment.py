from temporalio import workflow
from datetime import timedelta

with workflow.unsafe.imports_passed_through():
    from ..activities import (
        deploy_qlever,
        notify_email_deployed,
        get_qlever_storage_size,
        submit_qlever_index_fetch,
        watch_k8s_job_sync,
    )

from temporalio.common import RetryPolicy

NO_RETRY = RetryPolicy(maximum_attempts=1)
# Watching a K8s Job is pure polling, so it is safe to re-attach. This matters
# because the watcher heartbeats precisely so a multi-day build can outlive a
# worker restart -- but with maximum_attempts=1 a heartbeat timeout is terminal,
# and a routine `rollout restart` kills the workflow instead. Terminal outcomes
# (Job failed, Job missing) are raised non-retryable by the activity, so this
# only retries the act of watching.
WATCH_RETRY = RetryPolicy(
    initial_interval=timedelta(seconds=10),
    backoff_coefficient=2.0,
    maximum_interval=timedelta(minutes=1),
    maximum_attempts=20,
)

@workflow.defn
class QLeverDeploymentWorkflow:
    @workflow.run
    async def run(self, kg_config: dict, cpu: str, memory: str, mem_size: str, lakefs_action: dict, qlever_args: list = None) -> None:

        repo_id = lakefs_action.get('repository_id')
        branch_id = lakefs_action.get('tag_id') or lakefs_action.get('commit_id')

        pvc_storage_size = await workflow.execute_activity(
            get_qlever_storage_size,
            args=[repo_id, branch_id],
            start_to_close_timeout=timedelta(minutes=5),
            retry_policy=NO_RETRY
        )

        # Load the index onto the PVC before the server pod starts. The server
        # Deployment is a single container (no init container), so it expects the
        # index to already be there -- see server-deployment.j2.
        fetch_job = await workflow.execute_activity(
            submit_qlever_index_fetch,
            args=[kg_config, lakefs_action, pvc_storage_size],
            start_to_close_timeout=timedelta(minutes=15),
            retry_policy=NO_RETRY
        )
        if fetch_job:
            await workflow.execute_activity(
                watch_k8s_job_sync,
                args=[fetch_job, 5, "remote"],
                start_to_close_timeout=timedelta(hours=12),
                heartbeat_timeout=timedelta(minutes=5),
                retry_policy=WATCH_RETRY
            )

        # Deploy QLever Server
        await workflow.execute_activity(
            deploy_qlever,
            args=[kg_config, lakefs_action, cpu, memory, mem_size, pvc_storage_size, qlever_args],
            start_to_close_timeout=timedelta(minutes=30),
            retry_policy=NO_RETRY
        )

        # Email Notification regarding QLever Endpoint
        if kg_config.get('emails'):
             await workflow.execute_activity(
                 notify_email_deployed,
                 args=[kg_config['title'] + " (QLever Endpoint)",
                       lakefs_action.get('tag_id', 'latest'),
                       ",".join(kg_config['emails']),
                       # The shortname, separately: it keys the query page's
                       # `sources` filter and the /{shortname} endpoint path.
                       kg_config.get('shortname', '')],
                 start_to_close_timeout=timedelta(minutes=2),
                 retry_policy=NO_RETRY
             )
