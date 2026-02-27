import asyncio
from temporalio.worker import Worker
from .client import get_client
from log_util import LoggingUtil

from .activities import (
    run_k8s_job, 
    watch_k8s_job_sync, 
    deploy_fuseki, 
    deploy_ldf, 
    notify_slack, 
    notify_email_deployed,
    resolve_commit_details,
    download_file_lakefs,
    get_kg_config_from_git,
    download_input_files,
    upload_output_files,
    cleanup_local_files,
    send_review_email,
    prepare_qlever_job_specs,
    get_spider_config,
    create_local_dir
)

from .workflows import (
    HDTConversionWorkflow,
    Neo4jConversionWorkflow,
    DeploymentWorkflow,
    QLeverIndexWorkflow
)

logger = LoggingUtil.init_logging(__name__)

async def main():
    client = await get_client()
    
    # Define the task queue name (parallel to Celery queue)
    task_queue = "frink-temporal-queue"

    worker = Worker(
        client,
        task_queue=task_queue,
        workflows=[
            HDTConversionWorkflow,
            Neo4jConversionWorkflow,
            DeploymentWorkflow,
            QLeverIndexWorkflow
        ],
        activities=[
            run_k8s_job, 
            watch_k8s_job_sync, 
            deploy_fuseki, 
            deploy_ldf, 
            notify_slack, 
            notify_email_deployed,
            resolve_commit_details,
            download_file_lakefs,
            get_kg_config_from_git,
            download_input_files,
            upload_output_files,
            cleanup_local_files,
            send_review_email,
            prepare_qlever_job_specs,
            get_spider_config,
            create_local_dir
        ],
    )
    
    logger.info(f"Starting Temporal Worker on queue: {task_queue}")
    await worker.run()

if __name__ == "__main__":
    asyncio.run(main())
