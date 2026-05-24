from temporalio import workflow
from datetime import timedelta

with workflow.unsafe.imports_passed_through():
    from ..activities import (
        deploy_fuseki,
        deploy_ldf,
        notify_email_deployed,
        download_hdt_files_activity
    )

from temporalio.common import RetryPolicy
NO_RETRY = RetryPolicy(maximum_attempts=1)

@workflow.defn
class FusekiDeploymentWorkflow:
    @workflow.run
    async def run(self, kg_config: dict, cpu: str, memory: str, lakefs_action: dict, hdt_path: str = 'hdt/') -> None:
        # 0. Download HDT files
        await workflow.execute_activity(
            download_hdt_files_activity,
            args=[lakefs_action['repository_id'], lakefs_action['tag_id'], kg_config['shortname'], hdt_path],
            start_to_close_timeout=timedelta(minutes=360),
            retry_policy=NO_RETRY
        )

        # 1. Deploy Fuseki
        await workflow.execute_activity(
            deploy_fuseki,
            args=[kg_config, lakefs_action, cpu, memory],
            start_to_close_timeout=timedelta(minutes=20),
            retry_policy=NO_RETRY
        )

        # 2. Deploy LDF
        await workflow.execute_activity(
            deploy_ldf,
            args=[kg_config, lakefs_action],
            start_to_close_timeout=timedelta(minutes=10),
            retry_policy=NO_RETRY
        )

        # 3. Email Notification
        if kg_config.get('emails'):
             await workflow.execute_activity(
                 notify_email_deployed,
                 args=[kg_config['title'], lakefs_action.get('tag_id', 'latest'), ",".join(kg_config['emails'])],
                 start_to_close_timeout=timedelta(minutes=2),
                 retry_policy=NO_RETRY
             )
