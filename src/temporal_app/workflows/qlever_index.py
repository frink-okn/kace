import asyncio
from temporalio import workflow
from datetime import timedelta

with workflow.unsafe.imports_passed_through():
    from ..activities import (
        run_k8s_job,
        watch_k8s_job_sync,
        download_file_lakefs,
        prepare_qlever_job_specs,
    )

# Default timeouts
LONG_RUNNING_JOB_TIMEOUT = timedelta(hours=42)

from temporalio.common import RetryPolicy
NO_RETRY = RetryPolicy(maximum_attempts=1)

@workflow.defn
class QLeverIndexWorkflow:
    @workflow.run
    async def run(self, only_kg: list[str] = None) -> None:
        specs = await workflow.execute_activity(
            prepare_qlever_job_specs,
            args=[only_kg],
            start_to_close_timeout=timedelta(minutes=5),
            retry_policy=NO_RETRY
        )
        
        # Parallel downloads
        download_futures = []
        for dl in specs['downloads']:
            download_futures.append(
                workflow.execute_activity(
                    download_file_lakefs,
                    args=[dl['repo'], dl['remote_path'], dl['local_path']],
                    start_to_close_timeout=timedelta(hours=2),
                    retry_policy=NO_RETRY
                )
            )
        
        if download_futures:
            await asyncio.gather(*download_futures)
            
        # Run Job
        # celery passes command=["bash"], args=["-c", build_args]
        await workflow.execute_activity(
            run_k8s_job,
             args=[
                "qlever-index-job",
                "qlever-index-maintenance",
                "", # repo
                "", # branch
                ["bash"], # command
                ["-c", specs['build_command']], # args
                { # resources
                    "requests": {"cpu": "1", "memory": "4Gi"},
                    "limits": {"cpu": "8", "memory": "100Gi"}
                },
                {"STXXL_MEMORY": "40G"}, # env_vars
                [("data", "/shared")] # additional_volume_mounts
            ],
            start_to_close_timeout=LONG_RUNNING_JOB_TIMEOUT,
            retry_policy=NO_RETRY
        )
        
        await workflow.execute_activity(
            watch_k8s_job_sync,
            args=["qlever-index-maintenance"],
            start_to_close_timeout=LONG_RUNNING_JOB_TIMEOUT,
            retry_policy=NO_RETRY
        )
