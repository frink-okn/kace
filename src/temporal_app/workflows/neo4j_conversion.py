from temporalio import workflow
from datetime import timedelta

with workflow.unsafe.imports_passed_through():
    from ..activities import (
        run_k8s_job,
        watch_k8s_job_sync,
        get_kg_config_from_git,
        download_input_files,
        notify_slack,
        KG,
        app_config
    )
    from .hdt_conversion import HDTConversionWorkflow, HDTConversionInput

# Default timeouts
LONG_RUNNING_JOB_TIMEOUT = timedelta(hours=42)

from temporalio.common import RetryPolicy
NO_RETRY = RetryPolicy(maximum_attempts=1)

@workflow.defn
class Neo4jConversionWorkflow:
    @workflow.run
    async def run(self, action_payload: dict, rdf_mapping_config: str = None,
                  cpu: int = 1, memory: str = "28Gi", ephemeral: str = "512Mi",
                  java_opts: str = "-Xmx25G -Xms25G -Xss512m -XX:+UseParallelGC",) -> None:
        repo_id = action_payload['repository_id']
        branch_id = action_payload['branch_id']

        workflow.logger.info(f"Starting Neo4jConversionWorkflow for {repo_id}")

        # 1. Get KG config
        kg_config = await workflow.execute_activity(
            get_kg_config_from_git,
            repo_id,
            start_to_close_timeout=timedelta(minutes=5),
            retry_policy=NO_RETRY
        )
        kg_config: KG = KG(**kg_config)

        # Use rdf_mapping_config from KG config if not provided
        if not rdf_mapping_config:
            rdf_mapping_config = kg_config.frink_options.neo4j_conversion_config_path

        job_name_base = f"{action_payload['hook_id']}-{repo_id[:10]}-{branch_id}-{action_payload['commit_id'][:10]}"
        json_job_name = f"{job_name_base}-to-json"
        rdf_job_name = f"{job_name_base}-to-rdf"
        working_dir = f"/mnt/repo/{repo_id}/{branch_id}"

        # 2. Download dump files from LakeFS
        dump_files_list = await workflow.execute_activity(
            download_input_files,
            args=[repo_id, branch_id, ['dump'], None, None, True],
            start_to_close_timeout=timedelta(hours=2),
            retry_policy=NO_RETRY
        )

        # 3. Download JSON files (if any already exist)
        json_dump_files_list = await workflow.execute_activity(
            download_input_files,
            args=[repo_id, branch_id, ['json', 'json.gz', 'json.zst'], None, None, False],
            start_to_close_timeout=timedelta(hours=2),
            retry_policy=NO_RETRY
        )

        source_files = ""

        if not json_dump_files_list:
            # 4a. Run dump to JSON
            await workflow.execute_activity(
                run_k8s_job,
                args=[
                    "neo4j-json-job",
                    json_job_name,
                    repo_id,
                    branch_id,
                    None,
                    dump_files_list,
                    {"limits": {"cpu": cpu, "memory": memory, "ephemeral-storage": ephemeral}},
                    {"JAVA_OPTIONS": java_opts, "WORKING_DIR": working_dir}
                ],
                start_to_close_timeout=timedelta(minutes=10),
                retry_policy=NO_RETRY
            )
            await workflow.execute_activity(
                watch_k8s_job_sync,
                args=[json_job_name],
                start_to_close_timeout=LONG_RUNNING_JOB_TIMEOUT,
                retry_policy=NO_RETRY
            )
            source_files = "neo4j-export/neo4j-apoc-export.json"
        else:
            if len(json_dump_files_list) > 1:
                await workflow.execute_activity(
                    notify_slack,
                    f"⚠ Warning: {repo_id} contains multiple JSON exports, using first one.",
                    start_to_close_timeout=timedelta(minutes=1),
                    retry_policy=NO_RETRY
                )
            source_files = json_dump_files_list[0]

        neo4j_export_json_location = f"{working_dir.rstrip('/')}/{source_files.lstrip('/')}"

        # 5. Run RDF conversion
        await workflow.execute_activity(
            run_k8s_job,
             args=[
                "neo4j-rdf-job",
                rdf_job_name,
                repo_id,
                branch_id,
                None,
                ["-i", neo4j_export_json_location, "-c", rdf_mapping_config, "-w", working_dir],
                {"limits": {"cpu": 3, "memory": "20Gi"}},
                {"JAVA_OPTIONS": "-Xmx20G -XX:+UseParallelGC", "WORKING_DIR": working_dir}
            ],
            start_to_close_timeout=timedelta(minutes=10),
            retry_policy=NO_RETRY
        )
        await workflow.execute_activity(
            watch_k8s_job_sync,
            args=[rdf_job_name],
            start_to_close_timeout=LONG_RUNNING_JOB_TIMEOUT,
            retry_policy=NO_RETRY
        )

        # 6. Notify Neo4j conversion complete
        await workflow.execute_activity(
            notify_slack,
            f"✔️ Neo4j dump converted to RDF for {repo_id}. Chaining into HDT conversion.",
            start_to_close_timeout=timedelta(minutes=1),
            retry_policy=NO_RETRY
        )

        # 7. Chain into HDT conversion as a child workflow
        # Pass the .nt file list directly — HDT workflow will skip download
        # and use these local files for conversion.
        local_dir = f"/{app_config.local_data_dir.lstrip('/').rstrip('/')}/{repo_id}/{branch_id}"
        nt_files = [f"{local_dir}/nt/graph.nt.gz"]

        await workflow.execute_child_workflow(
            HDTConversionWorkflow.run,
            HDTConversionInput(
                action_payload=action_payload,
                doc_path=kg_config.frink_options.documentation_path,
                cpu=1,
                memory="28Gi",
                ephemeral="512Mi",
                java_opts="-Xmx25G -Xms25G -Xss512m -XX:+UseParallelGC",
                mem_size="25G",
                convert_to_hdt=True,
                hdt_path="/",
                files_list=nt_files,  # triggers skip of download
            ),
            id=f"hdt-{job_name_base}",
            retry_policy=NO_RETRY
        )
        # Note: cleanup is handled by the HDT child workflow


