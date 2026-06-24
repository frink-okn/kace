from temporalio import workflow
from datetime import timedelta
from dataclasses import dataclass
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

# Skip output directories from previous runs to avoid re-merging them
OUTPUT_PREFIXES = ['qlever/', 'hdt/', 'nt/', 'void/']


@dataclass
class HDTConversionInput:
    action_payload: dict
    doc_path: str
    cpu: int
    pod_memory: str
    ephemeral: str
    java_opts: str
    program_memory: str
    convert_to_hdt: bool
    hdt_path: str
    exclude_files: list | None = None
    exclude_known_extension: list | None = None
    files_list: list | None = None


@workflow.defn
class HDTConversionWorkflow:
    @workflow.run
    async def run(self, input: HDTConversionInput) -> None:
        # All run inputs live on self.input; derived values are computed onto self below.
        self.input = input
        action_payload = input.action_payload

        self.repo_id = action_payload['repository_id']
        self.branch_id = action_payload['branch_id']
        self.job_name = f"{action_payload['hook_id']}-{self.repo_id[:10]}-{self.branch_id.replace('_','-')[:10]}-{action_payload['commit_id'][:10]}"
        self.working_dir = f"/mnt/repo/{self.repo_id}/{self.branch_id}"
        self.local_dir = f"/{app_config.local_data_dir.lstrip('/').rstrip('/')}/{self.repo_id}/{self.branch_id}"

        # Fetch KG config, then derive the shared state every phase reads.
        kg_config = await workflow.execute_activity(
            get_kg_config_from_git,
            self.repo_id,
            start_to_close_timeout=timedelta(minutes=5),
            retry_policy=NO_RETRY,
        )
        self.kg_config: KG = KG(**kg_config)
        self.kg_title = self.kg_config.shortname
        workflow.logger.info(f"Starting HDTConversionWorkflow for {self.kg_title}")

        # Resources + env reused by every K8s job in this run.
        self.resources = {
            "requests": {"cpu": str(input.cpu), "memory": input.pod_memory, "ephemeral-storage": input.ephemeral},
            "limits": {"cpu": str(input.cpu), "memory": input.pod_memory, "ephemeral-storage": input.ephemeral},
        }
        self.env_base = {
            "GH_HANDLES": ",".join(self.kg_config.github_handles),
            "WORKING_DIR": self.working_dir,
            "REPO_NAME": self.repo_id,
            "COMMIT_ID": action_payload['source_ref'],
            "KG_NAME": self.kg_title,
        }
        self.dataset_uri = f"https://purl.org/okn/frink/kg/{self.kg_title}"

        # 2. Download input files (skipped if files_list already provided)
        file_list = await self._download_inputs(input.files_list, input.exclude_files, input.exclude_known_extension)

        # 3. Run HDT and NT conversion jobs
        await self._run_hdt_convert(file_list)
        await self._run_nt_merge()
        await self._run_riot_validate()

        # 4. VOID job and build-version TTL
        await self._run_void()
        await self._run_void_labels()
        await self._write_build_version()

        # 5. QLever index job
        await self._run_qlever_index()

        # 6. Documentation job
        # await self._run_documentation(self.input.doc_path)

        # 7. Upload
        upload_result, upload_void, void_repo = await self._upload_outputs()

        # 8. Notify
        await self._notify_and_email(upload_result, upload_void, void_repo)

        # 9. Cleanup local files
        await self._cleanup()

    async def _run_job_and_wait(self, *, job_type, job_name, command, args,
                                env_vars, watch_timeout, resources=None) -> None:
        """
        Submit a K8s Job and block until it reaches a terminal state.

        `run_k8s_job` returns as soon as the Job is submitted; `watch_k8s_job_sync`
        heartbeats while polling so the (potentially multi-hour) Job survives worker
        restarts.
        """
        await workflow.execute_activity(
            run_k8s_job,
            args=[job_type, job_name, self.repo_id, self.branch_id,
                  command, args, resources or self.resources, env_vars],
            start_to_close_timeout=timedelta(minutes=10),
            retry_policy=NO_RETRY,
        )
        await workflow.execute_activity(
            watch_k8s_job_sync,
            args=[job_name],
            start_to_close_timeout=watch_timeout,
            retry_policy=NO_RETRY,
        )

    async def _download_inputs(self, files_list, exclude_files, exclude_known_extension) -> list[str]:
        if files_list:
            workflow.logger.info(f"Using provided files_list ({len(files_list)} files) — skipping download")
            return [x.replace('/local/', '/mnt/repo/') for x in files_list]

        if self.input.convert_to_hdt:
            files_list = await workflow.execute_activity(
                download_input_files,
                args=[self.repo_id, self.branch_id, None, exclude_files, exclude_known_extension, True, OUTPUT_PREFIXES],
                start_to_close_timeout=timedelta(hours=24),
                retry_policy=NO_RETRY,
            )
        else:
            files_list = await workflow.execute_activity(
                download_input_files,
                args=[self.repo_id, self.branch_id, ['hdt'], exclude_files, exclude_known_extension, True, OUTPUT_PREFIXES],
                start_to_close_timeout=timedelta(hours=24),
                heartbeat_timeout=timedelta(minutes=15),
                retry_policy=NO_RETRY,
            )
        return [self.working_dir + '/' + x for x in files_list]

    async def _run_hdt_convert(self, file_list) -> None:
        hdt_convert_args = ['create'] + file_list + [
            '--temp-dir',
            self.working_dir + '/hdt-tmp/',
            '--index',
            '--memory-limit',
            self.input.program_memory,
            '-v',
            '--output',
            self.working_dir + '/hdt/graph.hdt',
        ]

        await workflow.execute_activity(
            create_local_dir,
            args=[self.local_dir + '/hdt'],
            start_to_close_timeout=timedelta(minutes=1),
            retry_policy=NO_RETRY,
        )
        await workflow.execute_activity(
            create_local_dir,
            args=[self.local_dir + '/nt'],
            start_to_close_timeout=timedelta(minutes=1),
            retry_policy=NO_RETRY,
        )

        await self._run_job_and_wait(
            job_type="hdtc-job",
            job_name=self.job_name,
            command=None,
            args=hdt_convert_args,
            env_vars={**self.env_base, "JAVA_OPTIONS": self.input.java_opts, "MEM_SIZE": self.input.program_memory},
            watch_timeout=LONG_RUNNING_JOB_TIMEOUT,
        )

    async def _run_nt_merge(self) -> None:
        nt_job_name = f"{self.job_name}-nt"
        nt_convert_args = ["-c", f"hdtc dump {self.working_dir}/hdt/graph.hdt | gzip > {self.working_dir}/nt/graph.nt.gz"]
        await self._run_job_and_wait(
            job_type="hdtc-job",
            job_name=nt_job_name,
            command=["/bin/sh"],
            args=nt_convert_args,
            env_vars={**self.env_base, "JAVA_OPTIONS": self.input.java_opts},
            watch_timeout=LONG_RUNNING_JOB_TIMEOUT,
        )

    async def _run_riot_validate(self) -> None:
        riot_job_name = f"{self.job_name}-riot-validate"
        await self._run_job_and_wait(
            job_type="nt-merge-job",
            job_name=riot_job_name,
            command=["/bin/validate-nt.sh"],
            args=None,
            env_vars=dict(self.env_base),
            watch_timeout=LONG_RUNNING_JOB_TIMEOUT,
        )

    async def _run_void(self) -> None:
        real_hdt_path = f"{self.working_dir}/hdt" if self.input.convert_to_hdt else f"{self.working_dir}/{self.input.hdt_path}"
        void_job_name = f"{self.job_name}-void"
        void_input = f"{real_hdt_path}/graph.hdt"
        void_output = f"{real_hdt_path}/void.nt.pre"
        await self._run_job_and_wait(
            job_type="hdtc-job",
            job_name=void_job_name,
            command=None,
            args=["void", void_input, "--dataset-uri", self.dataset_uri, "-o", void_output],
            env_vars=None,
            watch_timeout=timedelta(hours=6),
        )

    async def _run_void_labels(self) -> None:
        real_hdt_path = f"{self.working_dir}/hdt" if self.input.convert_to_hdt else f"{self.working_dir}/{self.input.hdt_path}"
        real_hdt = f"{real_hdt_path}/graph.hdt"
        void_job_name = f"{self.job_name}-void-labels"
        void_input = f"{real_hdt_path}/void.nt.pre"
        void_output = f"{real_hdt_path}/void.nt"
        await self._run_job_and_wait(
            job_type="hdtc-job",
            job_name=void_job_name,
            command=None,
            args=["python", "void_add_labels_descriptions.py", "--input", void_input, "--output", void_output, real_hdt],
            env_vars=None,
            watch_timeout=timedelta(hours=6),
        )

    async def _write_build_version(self) -> None:
        if self.input.action_payload.get('commit_id'):
            version = self.input.action_payload['commit_id'][:7]
        else:
            version = "NA"

        now_dt = workflow.now()
        timestamp_str = now_dt.isoformat()
        month_year_str = now_dt.strftime("%b %Y")  # e.g., "Nov 2025"

        pav_ttl_path = f"{self.local_dir}/hdt/void.nt"

        version_tag = await workflow.execute_activity(
            get_future_tag,
            args=[self.repo_id],
            start_to_close_timeout=timedelta(minutes=2),
            retry_policy=NO_RETRY,
        )
        nt_content = (
            f"<{self.dataset_uri}> <http://purl.org/pav/version> \"{version_tag}\" .\n"
            f"<{self.dataset_uri}> <http://purl.org/pav/lastUpdatedOn> \"{timestamp_str}\"^^<http://www.w3.org/2001/XMLSchema#dateTime> .\n"
            f"<{self.dataset_uri}> <http://purl.org/dc/terms/modified> \"{month_year_str}\" .\n"
        )

        await workflow.execute_activity(
            create_local_file,
            # file path, content, append
            args=[pav_ttl_path, nt_content, True],
            start_to_close_timeout=timedelta(minutes=1),
            retry_policy=NO_RETRY,
        )

    async def _run_qlever_index(self) -> None:
        unzip_stream = f"-f <(gunzip -c {self.working_dir}/nt/graph.nt.gz) "
        if self.repo_id == "secure-chain-kg":
            unzip_stream = f"-f <(gunzip -c {self.working_dir}/nt/graph.nt.gz | grep -v hasNAICSCodeValue) "
        if self.repo_id == "spatial-kg":
            unzip_stream = f"-f <(gunzip -c {self.working_dir}/nt/graph.nt.gz | grep -v '<http://stko-kwg.geog.ucsb.edu/lod/ontology/cellID>') "

        qlever_job_name = f"{self.job_name}-qlever"
        qlever_cmd = (
            f"ulimit -Sn 1048576; "
            f"mkdir -p {self.working_dir}/qlever; "
            f"cd {self.working_dir}/qlever; "
            f"IndexBuilderMain -i {self.kg_title} -s /qlever/frink-qlever.settings.json "
            f"{unzip_stream}"
            f"-g {self.dataset_uri} -F nt "
            f"-f {self.working_dir}/hdt/void.nt "
            f"-g {self.dataset_uri}#void -F nt  --stxxl-memory {self.input.program_memory} "
        )

        await workflow.execute_activity(
            create_local_dir,
            args=[self.local_dir + '/qlever', True],
            start_to_close_timeout=timedelta(minutes=1),
            retry_policy=NO_RETRY,
        )

        await self._run_job_and_wait(
            job_type="qlever-index-job",
            job_name=qlever_job_name,
            command=["bash"],
            args=["-c", qlever_cmd],
            env_vars={"STXXL_MEMORY": f"{self.input.program_memory}"},
            watch_timeout=LONG_RUNNING_JOB_TIMEOUT,
        )

    # 6. Documentation job — disabled. Re-enable by uncommenting this method and its
    # call in run() (between _run_qlever_index and _upload_outputs). NOTE: the original
    # inline version was submit-only; _run_job_and_wait also watches the job to terminal,
    # which is almost certainly the desired behavior on re-enable.
    # async def _run_documentation(self, doc_path) -> None:
    #     real_hdt_path = f"{self.working_dir}/hdt" if self.input.convert_to_hdt else f"{self.working_dir}/{self.input.hdt_path}"
    #     await self._run_job_and_wait(
    #         job_type="documentation-job",
    #         job_name=f"{self.job_name}-doc",
    #         command=None,
    #         args=[doc_path, real_hdt_path, self.kg_title, f"{self.kg_title}-documentation-update"],
    #         env_vars={"WORKING_DIR": self.working_dir},
    #         watch_timeout=LONG_RUNNING_JOB_TIMEOUT,
    #     )

    async def _upload_outputs(self):
        hdt_location = f"{self.local_dir}/hdt"
        nt_location = f"{self.local_dir}/nt"
        qlever_location = f"{self.local_dir}/qlever"
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
            retry_policy=NO_RETRY,
        )
        if qlever_files:
            local_files.extend(qlever_files)

        upload_result = await workflow.execute_activity(
            upload_output_files,
            args=[self.repo_id, self.branch_id, local_files],
            start_to_close_timeout=timedelta(hours=2),
            retry_policy=NO_RETRY,
        )

        void_repo, void_branch = app_config.void_repo.split(':')
        files = [
            [f"{hdt_location}/void.nt", f"{self.repo_id}"]
        ]
        upload_void = await workflow.execute_activity(
            upload_output_files,
            args=[void_repo, self.branch_id, files],
            start_to_close_timeout=timedelta(hours=2),
            retry_policy=NO_RETRY,
        )

        return upload_result, upload_void, void_repo

    async def _notify_and_email(self, upload_result, upload_void, void_repo) -> None:
        await workflow.execute_activity(
            notify_slack,
            f"✔️ HDT & NT files uploaded for: {app_config.lakefs_public_url}repositories/{self.repo_id}/objects?ref={upload_result['stable_branch_name']}"
            f"Void uploaded to {app_config.lakefs_public_url}repositories/{void_repo}/objects?ref={upload_void['stable_branch_name']}",
            start_to_close_timeout=timedelta(minutes=1),
            retry_policy=NO_RETRY,
        )

        if self.kg_config.emails:
            email_to = ",".join(self.kg_config.emails)
            await workflow.execute_activity(
                send_review_email,
                args=[
                    email_to,
                    upload_result['stable_branch_name'],
                    upload_result['future_tag'],
                    self.repo_id
                ],
                start_to_close_timeout=timedelta(minutes=2),
                retry_policy=NO_RETRY,
            )

    async def _cleanup(self) -> None:
        await workflow.execute_activity(
            cleanup_local_files,
            self.repo_id,
            start_to_close_timeout=timedelta(minutes=5),
            retry_policy=NO_RETRY,
        )
