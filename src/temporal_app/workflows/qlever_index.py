"""Federated QLever index build.

Phase 0: GC stale per-build PVCs (orphans + previous past 24h TTL).
Phase 1: resolve refs+commits for every source repo (KGs + s2-builds).
Phase 2: short-circuit if no source commit has changed since the serving build.
Phase 3: allocate a new per-build RWO premium-ssd output PVC.
Phase 4: download all source files to /shared/qlever-source (pinned to refs).
Phase 5: submit the IndexBuilderMain Job (writes to the new output PVC).
Phase 6: watch the Job (heartbeated, multi-day safe).
Phase 7: write new state (serving=new, previous=old_serving, previous_marked_at=now).

The workflow does not perform server rollover; that is the responsibility of a
downstream workflow that consumes `state.build_id_serving`.
"""
import asyncio
from datetime import timedelta
from temporalio import workflow
from temporalio.common import RetryPolicy

with workflow.unsafe.imports_passed_through():
    from ..activities import (
        run_k8s_job,
        watch_k8s_job_sync,
        download_file_lakefs,
        prepare_qlever_job_specs,
        resolve_qlever_refs,
        read_qlever_state,
        write_qlever_state,
        create_qlever_index_pvc,
        gc_qlever_index_pvcs,
        notify_slack,
    )
    from config import config as app_config


# Phase-3 PVC creation, GC, and ConfigMap touches are quick.
QUICK_TIMEOUT       = timedelta(minutes=5)
# Per-file download. Wikidata can be huge.
DOWNLOAD_TIMEOUT    = timedelta(hours=12)
# Submitting the indexer Job returns immediately; the wait is in the watcher.
JOB_SUBMIT_TIMEOUT  = timedelta(minutes=10)
# Multi-day federated index build; watch activity heartbeats so worker bounces
# do not lose progress.
BUILD_WATCH_TIMEOUT = timedelta(days=7)
HEARTBEAT_TIMEOUT   = timedelta(minutes=5)

NO_RETRY = RetryPolicy(maximum_attempts=1)

INDEX_MOUNT_PATH = "/index"
SHARED_VOLUME_NAME = "data"


@workflow.defn
class QLeverIndexWorkflow:
    @workflow.run
    async def run(self, only_kg: list = None) -> dict:
        start_time = workflow.info().start_time          # deterministic across replays
        build_id   = start_time.strftime("%Y%m%d-%H%M%S")
        now_iso    = workflow.now().isoformat()
        scope_msg  = (f" (subset: {', '.join(only_kg)})" if only_kg else "")

        await workflow.execute_activity(
            notify_slack,
            args=[f"🚀 QLever federated index build {build_id} starting{scope_msg}."],
            start_to_close_timeout=QUICK_TIMEOUT,
            retry_policy=NO_RETRY,
        )

        try:
            return await self._run_phases(only_kg, build_id, now_iso)
        except Exception as e:
            await workflow.execute_activity(
                notify_slack,
                args=[f"❌ QLever federated index build {build_id} failed: {e}"],
                start_to_close_timeout=QUICK_TIMEOUT,
                retry_policy=NO_RETRY,
            )
            raise

    async def _run_phases(self, only_kg, build_id, now_iso) -> dict:
        # ── Phase 0: GC ────────────────────────────────────────────────────
        state = await workflow.execute_activity(
            read_qlever_state,
            start_to_close_timeout=QUICK_TIMEOUT,
            retry_policy=NO_RETRY,
        )
        gc_report = await workflow.execute_activity(
            gc_qlever_index_pvcs,
            args=[state, now_iso],
            start_to_close_timeout=QUICK_TIMEOUT,
            retry_policy=NO_RETRY,
        )
        if gc_report.get("previous_aged_out"):
            # Previous PVC was deleted; clear it from state so future GCs don't
            # keep looking for it.
            state["build_id_previous"] = None
            state["previous_marked_at"] = None

        # ── Phase 1: resolve refs ─────────────────────────────────────────
        refs = await workflow.execute_activity(
            resolve_qlever_refs,
            args=[only_kg],
            start_to_close_timeout=timedelta(minutes=15),
            retry_policy=NO_RETRY,
        )

        # Slack-report any KGs that were skipped due to missing source files.
        if refs.get("skipped"):
            lines = [
                f"  • `{s['shortname']}` ({s['repo']}@{s['ref']}:{s['remote_path']}) — {s['reason']}"
                for s in refs["skipped"]
            ]
            await workflow.execute_activity(
                notify_slack,
                args=[
                    "⚠️ QLever federated index: skipping KGs with no source file:\n"
                    + "\n".join(lines)
                ],
                start_to_close_timeout=QUICK_TIMEOUT,
                retry_policy=NO_RETRY,
            )

        # ── Phase 2: change detection ─────────────────────────────────────
        current_commits = {
            **{repo: meta["commit"] for repo, meta in refs["kg_refs"].items()},
            refs["s2_repo"]: refs["s2_commit"],
        }
        if (
            not only_kg
            and state.get("build_id_serving")
            and state.get("source_commits") == current_commits
            and state.get("image") == app_config.qlever_image
        ):
            await workflow.execute_activity(
                notify_slack,
                args=[
                    "ℹ️ QLever federated index: no source commit changes since "
                    f"{state['build_id_serving']}; skipping rebuild.",
                ],
                start_to_close_timeout=QUICK_TIMEOUT,
                retry_policy=NO_RETRY,
            )
            return {"status": "skipped", "build_id_serving": state["build_id_serving"]}

        # ── Phase 3: allocate output PVC ──────────────────────────────────
        pvc_name = await workflow.execute_activity(
            create_qlever_index_pvc,
            args=[build_id, app_config.qlever_image],
            start_to_close_timeout=QUICK_TIMEOUT,
            retry_policy=NO_RETRY,
        )

        # ── Phase 4: downloads ────────────────────────────────────────────
        specs = await workflow.execute_activity(
            prepare_qlever_job_specs,
            args=[refs["kg_refs"], refs["s2_tag"], only_kg],
            start_to_close_timeout=QUICK_TIMEOUT,
            retry_policy=NO_RETRY,
        )
        # Bound concurrent downloads to avoid overloading LakeFS. Each file
        # also fans out internally into PARALLEL_PARTS_DEFAULT byte-range
        # requests, so total in-flight HTTP = concurrency × parts.
        download_sem = asyncio.Semaphore(app_config.qlever_download_concurrency)

        async def bounded_download(dl):
            async with download_sem:
                return await workflow.execute_activity(
                    download_file_lakefs,
                    args=[dl["repo"], dl["remote_path"], dl["local_path"], dl.get("ref")],
                    start_to_close_timeout=DOWNLOAD_TIMEOUT,
                    retry_policy=NO_RETRY,
                )

        if specs["downloads"]:
            await asyncio.gather(*[bounded_download(dl) for dl in specs["downloads"]])

        # ── Phase 5: submit indexer Job ───────────────────────────────────
        job_name = f"qlever-index-{build_id}"
        await workflow.execute_activity(
            run_k8s_job,
            args=[
                "qlever-index-job",                              # job_type
                job_name,                                        # job_name
                "",                                              # repo
                "",                                              # branch
                ["bash"],                                        # command
                ["-c", specs["build_command"]],                  # args
                {                                                # resources
                    "requests": {"cpu": "1", "memory": "4Gi"},
                    "limits": {
                        "cpu":    app_config.qlever_indexer_cpu,
                        "memory": app_config.qlever_indexer_memory,
                    },
                },
                {"STXXL_MEMORY": app_config.qlever_indexer_stxxl_memory},  # env_vars
                None,                                            # additional_volume_mounts (none — shared PVC mounted via extra_pvcs below)
                app_config.qlever_image,                         # image override
                [                                                # extra_pvcs
                    {                                            # shared PVC: source files written by worker (same PVC, same path)
                        "name":       "shared-source",
                        "claim":      app_config.shared_pvc_name,
                        "mount_path": "/shared",
                        "read_only":  True,
                    },
                    {                                            # per-build output PVC
                        "name":       "index",
                        "claim":      pvc_name,
                        "mount_path": INDEX_MOUNT_PATH,
                        "read_only":  False,
                    },
                ],
                True,                                            # read_only_default_mount: legacy /mnt/repo mount stays RO
                {                                                # pod_security_context: own the freshly-mounted /index PVC
                    "run_as_user":  0,
                    "run_as_group": 0,
                    "fs_group":     0,
                },
            ],
            start_to_close_timeout=JOB_SUBMIT_TIMEOUT,
            retry_policy=NO_RETRY,
        )

        # ── Phase 6: watch Job (long-running, heartbeated) ────────────────
        await workflow.execute_activity(
            watch_k8s_job_sync,
            args=[job_name],
            start_to_close_timeout=BUILD_WATCH_TIMEOUT,
            heartbeat_timeout=HEARTBEAT_TIMEOUT,
            retry_policy=NO_RETRY,
        )

        # ── Phase 7: write new state ──────────────────────────────────────
        old_serving = state.get("build_id_serving")
        new_state = {
            "build_id_serving":   build_id,
            "build_id_previous":  old_serving,
            "previous_marked_at": now_iso if old_serving else None,
            "source_commits":     current_commits,
            "image":              app_config.qlever_image,
        }
        await workflow.execute_activity(
            write_qlever_state,
            args=[new_state],
            start_to_close_timeout=QUICK_TIMEOUT,
            retry_policy=NO_RETRY,
        )

        await workflow.execute_activity(
            notify_slack,
            args=[
                f"✅ QLever federated index build {build_id} complete. "
                f"PVC `{pvc_name}` ready for rollover.",
            ],
            start_to_close_timeout=QUICK_TIMEOUT,
            retry_policy=NO_RETRY,
        )

        return {
            "status":           "built",
            "build_id":         build_id,
            "pvc":              pvc_name,
            "gc_report":        gc_report,
            "previous_build":   old_serving,
        }
