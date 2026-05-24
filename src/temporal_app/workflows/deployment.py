from temporalio import workflow
from datetime import timedelta

with workflow.unsafe.imports_passed_through():
    from ..activities import (
        run_k8s_job,
        deploy_fuseki,
        deploy_ldf,
        notify_email_deployed,
        resolve_commit_details,
        get_spider_config
    )

# Default timeouts
LONG_RUNNING_JOB_TIMEOUT = timedelta(hours=42)

from temporalio.common import RetryPolicy
NO_RETRY = RetryPolicy(maximum_attempts=1)

@workflow.defn
class DeploymentWorkflow:
    @workflow.run
    async def run(self, kg_config: dict, cpu: str, memory: str, lakefs_action: dict) -> None:

        # Deploy Fuseki
        await workflow.execute_activity(
            deploy_fuseki,
            args=[kg_config, lakefs_action, cpu, memory],
            start_to_close_timeout=timedelta(minutes=20),
            retry_policy=NO_RETRY
        )

        # Deploy LDF
        await workflow.execute_activity(
            deploy_ldf,
            args=[kg_config, lakefs_action],
            start_to_close_timeout=timedelta(minutes=10),
            retry_policy=NO_RETRY
        )

        # Run Spider Job
        kg_name = kg_config.get('shortname', 'unknown')
        commit_id = lakefs_action.get('commit_id')
        job_name = f"spider-cl-{kg_name}-{commit_id[:10]}"

        commit_details = await workflow.execute_activity(
            resolve_commit_details,
            args=[lakefs_action['repository_id'], commit_id],
            start_to_close_timeout=timedelta(minutes=2),
            retry_policy=NO_RETRY
        )

        # Fetch spider server config via activity (can't access config directly in workflow)
        spider_config = await workflow.execute_activity(
            get_spider_config,
            start_to_close_timeout=timedelta(seconds=30),
            retry_policy=NO_RETRY
        )

        await workflow.execute_activity(
            run_k8s_job,
            args=[
                "spider-job",
                job_name,
                lakefs_action['repository_id'],
                commit_id,
                None,
                [
                    "/app/spider-client.py",
                    "--commit-id", commit_id,
                    "--graph-name", kg_name,
                    "--committer-name", commit_details['committer'],
                    "--ip", spider_config['ip'],
                    "--port", str(spider_config['port'])
                ],
                None,
                None
            ],
            start_to_close_timeout=timedelta(minutes=10),
            retry_policy=NO_RETRY
        )
        # Spider always fails ....
        # await workflow.execute_activity(
        #     watch_k8s_job_sync,
        #     args=[job_name],
        #     start_to_close_timeout=LONG_RUNNING_JOB_TIMEOUT,
        #     retry_policy=NO_RETRY
        # )

        # Email
        if kg_config.get('emails'):
             await workflow.execute_activity(
                 notify_email_deployed,
                 args=[kg_config['title'], lakefs_action['tag_id'], ",".join(kg_config['emails'])],
                 start_to_close_timeout=timedelta(minutes=2),
                 retry_policy=NO_RETRY
             )
