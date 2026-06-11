from temporalio import workflow
from datetime import timedelta
with workflow.unsafe.imports_passed_through():
    from ..activities import (
        run_k8s_job,
        watch_k8s_job_sync,
        get_kg_config_from_git,
        download_input_files,
        upload_output_files,
        cleanup_local_files,
        send_review_email,
        create_local_dir,
        create_local_file,
        get_qlever_index_files,
        notify_slack,
        get_future_tag,
        KG,
        app_config
    )

# Default timeouts
ACTIVITY_TIMEOUT = timedelta(minutes=60*2) # 2 hours
LONG_RUNNING_JOB_TIMEOUT = timedelta(hours=42)

# Fail immediately on first error, no retries
from temporalio.common import RetryPolicy
NO_RETRY = RetryPolicy(maximum_attempts=1)

@workflow.defn
class HDTConversionWorkflow:
    @workflow.run
    async def run(self, action_payload: dict, doc_path: str,
                  cpu: int = 1, memory: str = "28Gi", ephemeral: str = "512Mi",
                  java_opts: str = "-Xmx25G -Xms25G -Xss512m -XX:+UseParallelGC",
                  mem_size: str = "25G", convert_to_hdt: bool = True, hdt_path: str = "/",
                  exclude_files: list = None, exclude_known_extension: list = None,
                  files_list: list = None) -> None:



        # 1. Get KG config
        kg_config = await workflow.execute_activity(
            get_kg_config_from_git,
            action_payload['repository_id'],
            start_to_close_timeout=timedelta(minutes=5),
            retry_policy=NO_RETRY
        )
        kg_config: KG = KG(**kg_config)
        kg_title = kg_config.shortname
        workflow.logger.info(f"Starting HDTConversionWorkflow for {kg_title}")


        repo_id = action_payload['repository_id']
        branch_id = action_payload['branch_id']
        job_name = f"{action_payload['hook_id']}-{repo_id[:10]}-{branch_id.replace('_','-')[:10]}-{action_payload['commit_id'][:10]}"
        working_dir = f"/mnt/repo/{repo_id}/{branch_id}"
        local_dir = f"/{app_config.local_data_dir.lstrip('/').rstrip('/')}/{repo_id}/{branch_id}"

        # 2. Download input files from LakeFS (skip if files_list already provided)
        # Skip output directories from previous runs to avoid re-merging them
        OUTPUT_PREFIXES = ['qlever/', 'hdt/', 'nt/', 'void/']

        if files_list:
            file_list = [x.replace('/local/', '/mnt/repo/') for x in files_list ]
            workflow.logger.info(f"Using provided files_list ({len(files_list)} files) — skipping download")
        elif convert_to_hdt:
            files_list = await workflow.execute_activity(
                download_input_files,
                args=[repo_id, branch_id, None, exclude_files, exclude_known_extension, True, OUTPUT_PREFIXES],
                start_to_close_timeout=timedelta(hours=24),
                retry_policy=NO_RETRY
            )
            file_list = [working_dir + '/' + x for x in files_list ]
        else:
            files_list = await workflow.execute_activity(
                download_input_files,
                args=[repo_id, branch_id, ['hdt'], exclude_files, exclude_known_extension, True, OUTPUT_PREFIXES],
                start_to_close_timeout=timedelta(hours=24),
                heartbeat_timeout=timedelta(minutes=15),
                retry_policy=NO_RETRY
            )
            file_list = [working_dir + '/' + x for x in files_list ]

        # 3. Run HDT and NT conversion jobs

        # 3.1 HDT conversion job
        hdt_convert_args = ['create'] + file_list + [
            '--temp-dir',
            working_dir + '/hdt-tmp/',
            '--index',
            '--memory-limit',
            mem_size,
            '-v',
            '--output',
            working_dir + '/hdt/graph.hdt'
        ]

        await workflow.execute_activity(
            create_local_dir,
            args=[local_dir + '/hdt'],
            start_to_close_timeout=timedelta(minutes=1),
            retry_policy=NO_RETRY
        )

        await workflow.execute_activity(
            create_local_dir,
            args=[local_dir + '/nt'],
            start_to_close_timeout=timedelta(minutes=1),
            retry_policy=NO_RETRY
        )

        await workflow.execute_activity(
            run_k8s_job,
            args=[
                "hdtc-job",
                job_name,
                repo_id,
                branch_id,
                None, # command override
                hdt_convert_args, # command args
                { # resources
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
        # run hdt conversion.
        await workflow.execute_activity(
            watch_k8s_job_sync,
            args=[job_name],
            start_to_close_timeout=LONG_RUNNING_JOB_TIMEOUT,
            retry_policy=NO_RETRY
        )

        # 3.2 NT Merge Job (Takes the HDT as input)
        nt_job_name = f"{job_name}-nt"
        nt_convert_args = ["-c", f"hdtc dump {working_dir}/hdt/graph.hdt | gzip > {working_dir}/nt/graph.nt.gz"]

        await workflow.execute_activity(
            run_k8s_job,
            args=[
                "hdtc-job",
                nt_job_name,
                repo_id,
                branch_id,
                ["/bin/sh"], # command override
                nt_convert_args, # command args
                { # resources
                    "requests": {"cpu": str(cpu), "memory": memory, "ephemeral-storage": ephemeral},
                    "limits": {"cpu": str(cpu), "memory": memory, "ephemeral-storage": ephemeral}
                },
                {
                    "GH_HANDLES": ",".join(kg_config.github_handles),
                    "JAVA_OPTIONS": java_opts,
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
            args=[nt_job_name],
            start_to_close_timeout=LONG_RUNNING_JOB_TIMEOUT,
            retry_policy=NO_RETRY
        )

        # 3.3 RIOT Validation Job
        riot_job_name = f"{job_name}-riot-validate"
        await workflow.execute_activity(
            run_k8s_job,
            args=[
                "nt-merge-job",
                riot_job_name,
                repo_id,
                branch_id,
                ["/bin/validate-nt.sh"],  # command override
                None,  # no extra args — script reads env vars
                {
                    "requests": {"cpu": str(cpu), "memory": memory, "ephemeral-storage": ephemeral},
                    "limits": {"cpu": str(cpu), "memory": memory, "ephemeral-storage": ephemeral}
                },
                {
                    "GH_HANDLES": ",".join(kg_config.github_handles),
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
            args=[riot_job_name],
            start_to_close_timeout=LONG_RUNNING_JOB_TIMEOUT,
            retry_policy=NO_RETRY
        )

        # Determine HDT path
        real_hdt_path = f"{working_dir}/hdt" if convert_to_hdt else f"{working_dir}/{hdt_path}"

        # 4. VOID Job
        void_job_name = f"{job_name}-void"
        void_input = f"{real_hdt_path}/graph.hdt"
        void_output = f"{real_hdt_path}/void.nt"
        dataset_uri = f"https://purl.org/okn/frink/kg/{kg_title}"

        await workflow.execute_activity(
            run_k8s_job,
            args=[
                "hdtc-job",
                void_job_name,
                repo_id,
                branch_id,
                None,
                ["void", void_input, "--dataset-uri", dataset_uri, "-o", void_output],
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

        # 4.5 Generate build version TTL
        if action_payload.get('commit_id'):
            version = action_payload['commit_id'][:7]
        else:
            version = "NA"

        now_dt = workflow.now()
        timestamp_str = now_dt.isoformat()
        month_year_str = now_dt.strftime("%b %Y")  # e.g., "Nov 2025"

        pav_ttl_path = f"{local_dir}/hdt/void.nt"


        version_tag = await workflow.execute_activity(
            get_future_tag,
            args=[repo_id],
            start_to_close_timeout=timedelta(minutes=2),
            retry_policy=NO_RETRY
        )
        nt_content = (
            f"<{dataset_uri}> <http://purl.org/pav/version> \"{version_tag}\" .\n"            
            f"<{dataset_uri}> <http://purl.org/pav/lastUpdatedOn> \"{timestamp_str}\"^^<http://www.w3.org/2001/XMLSchema#dateTime> .\n"
            f"<{dataset_uri}> <http://purl.org/dc/terms/modified> \"{month_year_str}\" .\n"
        )

        await workflow.execute_activity(
            create_local_file,
            # file path , content, append
            args=[pav_ttl_path, nt_content, True],
            start_to_close_timeout=timedelta(minutes=1),
            retry_policy=NO_RETRY
        )

        unzip_stream = f"-f <(gunzip -c {working_dir}/nt/graph.nt.gz) "
        if repo_id == "secure-chain-kg":
            unzip_stream = f"-f <(gunzip -c {working_dir}/nt/graph.nt.gz | grep -v hasNAICSCodeValue) "
        if repo_id == "spatial-kg":
            unzip_stream = f"-f <(gunzip -c {working_dir}/nt/graph.nt.gz | grep -v '<http://stko-kwg.geog.ucsb.edu/lod/ontology/cellID>') "

        # 5 QLever Index Job
        qlever_job_name = f"{job_name}-qlever"
        qlever_cmd = (
            f"ulimit -Sn 1048576; "
            f"mkdir -p {working_dir}/qlever; "
            f"cd {working_dir}/qlever; "
            f"IndexBuilderMain -i {kg_title} -s /qlever/frink-qlever.settings.json "
            f"{unzip_stream}"
            f"-g {dataset_uri} -F nt "
            f"-f {working_dir}/hdt/void.nt "
            f"-g {dataset_uri}#void -F nt  --stxxl-memory {mem_size} "
        )

        await workflow.execute_activity(
            create_local_dir,
            args=[local_dir + '/qlever', True],
            start_to_close_timeout=timedelta(minutes=1),
            retry_policy=NO_RETRY
        )

        await workflow.execute_activity(
            run_k8s_job,
            args=[
                "qlever-index-job",
                qlever_job_name,
                repo_id,
                branch_id,
                ["bash"],  # command override
                ["-c", qlever_cmd],  # command args
                {  # resources
                    "requests": {"cpu": str(cpu), "memory": memory, "ephemeral-storage": ephemeral},
                    "limits": {"cpu": str(cpu), "memory": memory, "ephemeral-storage": ephemeral}
                },
                {
                    "STXXL_MEMORY": f"{mem_size}"
                }
            ],
            start_to_close_timeout=timedelta(minutes=10),
            retry_policy=NO_RETRY
        )
        await workflow.execute_activity(
            watch_k8s_job_sync,
            args=[qlever_job_name],
            start_to_close_timeout=LONG_RUNNING_JOB_TIMEOUT,
            retry_policy=NO_RETRY
        )

        # 6. Documentation Job
        # doc_job_name = f"{job_name}-doc"
        # await workflow.execute_activity(
        #     run_k8s_job,
        #     args=[
        #         "documentation-job",
        #         doc_job_name,
        #         repo_id,
        #         branch_id,
        #         None,
        #         [doc_path, real_hdt_path, kg_title, f"{kg_title}-documentation-update"],
        #         {
        #             "requests": {"cpu": str(cpu), "memory": memory, "ephemeral-storage": ephemeral},
        #             "limits": {"cpu": str(cpu), "memory": memory, "ephemeral-storage": ephemeral}
        #         },
        #         {"WORKING_DIR": working_dir}
        #     ],
        #     start_to_close_timeout=timedelta(minutes=10),
        #     retry_policy=NO_RETRY
        # )

        # 7. Upload output files to LakeFS
        hdt_location = f"{local_dir}/hdt"
        nt_location = f"{local_dir}/nt"
        qlever_location = f"{local_dir}/qlever"
        local_files = [
            [f"{hdt_location}/graph.hdt", "hdt"],
            [f"{hdt_location}/graph.hdt.index.v1-1", "hdt"],
            [f"{nt_location}/graph.nt.gz", "nt"],
            [f"{hdt_location}/void.nt", "void"]
        ]

        qlever_files = await workflow.execute_activity(
            get_qlever_index_files,
            args=[qlever_location],
            start_to_close_timeout=timedelta(minutes=1),
            retry_policy=NO_RETRY
        )
        if qlever_files:
            local_files.extend(qlever_files)

        upload_result = await workflow.execute_activity(
            upload_output_files,
            args=[repo_id, branch_id, local_files],
            start_to_close_timeout=timedelta(hours=2),
            retry_policy=NO_RETRY
        )

        void_repo, void_branch = app_config.void_repo.split(':')
        files = [
            [f"{hdt_location}/void.nt", f"{repo_id}"]
        ]
        upload_void = await workflow.execute_activity(
            upload_output_files,
            args=[void_repo, branch_id, files],
            start_to_close_timeout=timedelta(hours=2),
            retry_policy=NO_RETRY
        )


        # 7. Notify
        await workflow.execute_activity(
            notify_slack,
            f"✔️ HDT & NT files uploaded for: {app_config.lakefs_public_url}repositories/{repo_id}/objects?ref={upload_result['stable_branch_name']}"
            f"Void uploaded to {app_config.lakefs_public_url}repositories/{void_repo}/objects?ref={upload_void['stable_branch_name']}",
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

        # 8. Cleanup local files
        await workflow.execute_activity(
            cleanup_local_files,
            repo_id,
            start_to_close_timeout=timedelta(minutes=5),
            retry_policy=NO_RETRY
        )
