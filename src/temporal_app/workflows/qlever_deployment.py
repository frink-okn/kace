from temporalio import workflow
from datetime import timedelta

with workflow.unsafe.imports_passed_through():
    from ..activities import (
        deploy_qlever,
        notify_email_deployed,
        get_qlever_storage_size
    )

from temporalio.common import RetryPolicy

NO_RETRY = RetryPolicy(maximum_attempts=1)

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
                 args=[kg_config['title'] + " (QLever Endpoint)", lakefs_action.get('tag_id', 'latest'), ",".join(kg_config['emails'])],
                 start_to_close_timeout=timedelta(minutes=2),
                 retry_policy=NO_RETRY
             )
