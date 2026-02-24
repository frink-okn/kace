from temporalio import workflow
from temporalio.common import RetryPolicy
from datetime import timedelta
with workflow.unsafe.imports_passed_through():
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
        KG,
        app_config
    )
import asyncio

# Default timeouts
ACTIVITY_TIMEOUT = timedelta(minutes=60*2) # 2 hours
LONG_RUNNING_JOB_TIMEOUT = timedelta(hours=42)

# Fail immediately on first error, no retries
NO_RETRY = RetryPolicy(maximum_attempts=1)

@workflow.defn
class HDTConversionWorkflow:
    @workflow.run
    async def run(self, action_payload: dict, doc_path: str, kg_title: str,
                  cpu: int = 1, memory: str = "28Gi", ephemeral: str = "512Mi",
                  java_opts: str = "-Xmx25G -Xms25G -Xss512m -XX:+UseParallelGC",
                  mem_size: str = "25G", convert_to_hdt: bool = True, hdt_path: str = "/",
                  exclude_files: list = None, exclude_known_extension: list = None,
                  files_list: list = None) -> None:

        workflow.logger.info(f"Starting HDTConversionWorkflow for {kg_title}")

        # 1. Get KG config
        kg_config = await workflow.execute_activity(
            get_kg_config_from_git,
            action_payload['repository_id'],
            start_to_close_timeout=timedelta(minutes=5),
            retry_policy=NO_RETRY
        )
        kg_config: KG = KG(**kg_config)

        repo_id = action_payload['repository_id']
        branch_id = action_payload['branch_id']
        job_name = f"{action_payload['hook_id']}-{repo_id[:10]}-{branch_id.replace('_','-')[:10]}-{action_payload['commit_id'][:10]}"
        working_dir = f"/mnt/repo/{repo_id}/{branch_id}"
        local_dir = f"/{app_config.local_data_dir.lstrip('/').rstrip('/')}/{repo_id}/{branch_id}"

        # 2. Download input files from LakeFS (skip if files_list already provided)
        if files_list:
            workflow.logger.info(f"Using provided files_list ({len(files_list)} files) — skipping download")
        elif convert_to_hdt:
            files_list = await workflow.execute_activity(
                download_input_files,
                args=[repo_id, branch_id, None, exclude_files, exclude_known_extension, True],
                start_to_close_timeout=timedelta(hours=2),
                retry_policy=NO_RETRY
            )
        else:
            files_list = await workflow.execute_activity(
                download_input_files,
                args=[repo_id, branch_id, ['hdt'], exclude_files, exclude_known_extension, True],
                start_to_close_timeout=timedelta(hours=2),
                retry_policy=NO_RETRY
            )

        # 3. Run HDT conversion job
        if convert_to_hdt:
            await workflow.execute_activity(
                run_k8s_job,
                args=[
                    "hdt-job",
                    job_name,
                    repo_id,
                    branch_id,
                    None,
                    files_list,
                    {
                        "requests": {"cpu": str(cpu), "memory": memory, "ephemeral-storage": ephemeral},
                        "limits": {"cpu": str(cpu), "memory": memory, "ephemeral-storage": ephemeral}
                    },
                    {
                        "GH_HANDLES": ",".join(kg_config.github_handles),
                        "JAVA_OPTIONS": java_opts,
                        "MEM_SIZE": mem_size,
                        "WORKING_DIR": working_dir,
                        "REPO_NAME": repo_id,
                        "COMMIT_ID": action_payload['source_ref'],
                        "KG_NAME": kg_title,
                    }
                ],
                start_to_close_timeout=timedelta(minutes=10),
                retry_policy=NO_RETRY
            )

            await workflow.execute_activity(
                watch_k8s_job_sync,
                args=[job_name],
                start_to_close_timeout=LONG_RUNNING_JOB_TIMEOUT,
                retry_policy=NO_RETRY
            )

        # Determine HDT path
        real_hdt_path = f"{working_dir}/hdt" if convert_to_hdt else f"{working_dir}/{hdt_path}"

        # 4. VOID Job
        void_job_name = f"{job_name}-void"
        void_input = f"{real_hdt_path}/graph.hdt"
        void_output = f"{real_hdt_path}/void.ttl"
        dataset_uri = f"https://purl.org/okn/frink/kg/{kg_title}"

        await workflow.execute_activity(
            run_k8s_job,
            args=[
                "void-job",
                void_job_name,
                repo_id,
                branch_id,
                None,
                [void_input, "--dataset-uri", dataset_uri, "-o", void_output],
                {
                    "requests": {"cpu": str(cpu), "memory": memory, "ephemeral-storage": ephemeral},
                    "limits": {"cpu": str(cpu), "memory": memory, "ephemeral-storage": ephemeral}
                },
                None
            ],
            start_to_close_timeout=timedelta(minutes=10),
            retry_policy=NO_RETRY
        )
        await workflow.execute_activity(
            watch_k8s_job_sync,
            args=[void_job_name],
            start_to_close_timeout=timedelta(hours=6),
            retry_policy=NO_RETRY
        )

        # 5. Documentation Job
        doc_job_name = f"{job_name}-doc"
        await workflow.execute_activity(
            run_k8s_job,
            args=[
                "documentation-job",
                doc_job_name,
                repo_id,
                branch_id,
                None,
                [doc_path, real_hdt_path, kg_title, f"{kg_title}-documentation-update"],
                {
                    "requests": {"cpu": str(cpu), "memory": memory, "ephemeral-storage": ephemeral},
                    "limits": {"cpu": str(cpu), "memory": memory, "ephemeral-storage": ephemeral}
                },
                {"WORKING_DIR": working_dir}
            ],
            start_to_close_timeout=timedelta(minutes=10),
            retry_policy=NO_RETRY
        )

        # 6. Upload output files to LakeFS
        hdt_location = f"{local_dir}/hdt"
        nt_location = f"{local_dir}/nt"
        local_files = [
            [f"{hdt_location}/graph.hdt", "hdt"],
            [f"{hdt_location}/graph.hdt.index.v1-1", "hdt"],
            [f"{nt_location}/graph.nt.gz", "nt"],
            [f"{hdt_location}/void.ttl", "void"],
        ] if convert_to_hdt else []

        upload_result = await workflow.execute_activity(
            upload_output_files,
            args=[repo_id, branch_id, local_files],
            start_to_close_timeout=timedelta(hours=2),
            retry_policy=NO_RETRY
        )

        # 7. Notify
        await workflow.execute_activity(
            notify_slack,
            f"✔️ HDT & NT file uploaded for {kg_title}. Branch: {upload_result['stable_branch_name']}",
            start_to_close_timeout=timedelta(minutes=1),
            retry_policy=NO_RETRY
        )

        if kg_config.emails:
            email_to = ",".join(kg_config.emails)
            await workflow.execute_activity(
                send_review_email,
                args=[
                    email_to,
                    upload_result['stable_branch_name'],
                    upload_result['future_tag'],
                    repo_id
                ],
                start_to_close_timeout=timedelta(minutes=2),
                retry_policy=NO_RETRY
            )

        await workflow.execute_activity(
            notify_slack,
            f"✔️ Conversion and Documentation complete for {kg_title}. Branch: {upload_result['stable_branch_name']}",
            start_to_close_timeout=timedelta(minutes=1),
            retry_policy=NO_RETRY
        )

        # 8. Cleanup local files
        await workflow.execute_activity(
            cleanup_local_files,
            repo_id,
            start_to_close_timeout=timedelta(minutes=5),
            retry_policy=NO_RETRY
        )

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
            args=[repo_id, branch_id, ['json', 'json.gz'], None, None, False],
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
            args=[
                action_payload,
                kg_config.frink_options.documentation_path,
                kg_config.shortname,
                # remaining params use defaults: cpu, memory, ephemeral, java_opts, mem_size,
                # convert_to_hdt, hdt_path, exclude_files, exclude_known_extension
                1, "28Gi", "512Mi",
                "-Xmx25G -Xms25G -Xss512m -XX:+UseParallelGC",
                "25G", True, "/",
                None, None,
                nt_files,  # files_list — triggers skip of download
            ],
            id=f"hdt-{job_name_base}",
            retry_policy=NO_RETRY
        )
        # Note: cleanup is handled by the HDT child workflow

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

