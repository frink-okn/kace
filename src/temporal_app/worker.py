import asyncio
from temporalio.worker import Worker
from .client import get_client
from log_util import LoggingUtil

from .activities import (
    run_k8s_job,
    watch_k8s_job_sync,
    deploy_fuseki,
    deploy_ldf,
    deploy_qlever, # Added
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
    create_local_dir,
    create_local_file,
    get_qlever_index_files,
    get_future_tag,
    get_qlever_storage_size,
    download_hdt_files_activity,
    fetch_all_kgs,
    get_lakefs_latest_commit_activity,
    resolve_kg_ref,
    resolve_latest_tag,
    resolve_qlever_refs,
    read_qlever_state,
    write_qlever_state,
    create_qlever_index_pvc,
    gc_qlever_index_pvcs,
    read_ldf_state,
    write_ldf_state,
    ensure_ldf_pvc,
    submit_ldf_sync_job,
    wait_ldf_sync_job,
    apply_ldf_config_and_rollout,
)

from .workflows import (
    HDTConversionWorkflow,
    Neo4jConversionWorkflow,
    DeploymentWorkflow,
    QLeverIndexWorkflow,
    QLeverDeploymentWorkflow,
    FusekiDeploymentWorkflow,
    LDFSyncWorkflow,
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
            QLeverIndexWorkflow,
            QLeverDeploymentWorkflow,
            FusekiDeploymentWorkflow, # Added
            LDFSyncWorkflow,
        ],
        activities=[
            run_k8s_job,
            watch_k8s_job_sync,
            deploy_fuseki,
            deploy_ldf,
            deploy_qlever, # Added
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
            create_local_dir,
            create_local_file,
            get_qlever_index_files,
            get_future_tag,
            get_qlever_storage_size,
            download_hdt_files_activity,
            fetch_all_kgs,
            get_lakefs_latest_commit_activity,
            resolve_kg_ref,
            resolve_latest_tag,
            resolve_qlever_refs,
            read_qlever_state,
            write_qlever_state,
            create_qlever_index_pvc,
            gc_qlever_index_pvcs,
            read_ldf_state,
            write_ldf_state,
            ensure_ldf_pvc,
            submit_ldf_sync_job,
            wait_ldf_sync_job,
            apply_ldf_config_and_rollout,
        ],
    )
    
    logger.info(f"Starting Temporal Worker on queue: {task_queue}")
    await worker.run()

if __name__ == "__main__":
    asyncio.run(main())
