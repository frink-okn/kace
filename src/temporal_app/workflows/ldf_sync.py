from temporalio import workflow
from datetime import timedelta

with workflow.unsafe.imports_passed_through():
    from ..activities import (
        fetch_all_kgs,
        resolve_kg_ref,
        read_ldf_state,
        write_ldf_state,
        ensure_ldf_pvc,
        submit_ldf_sync_job,
        wait_ldf_sync_job,
        apply_ldf_config_and_rollout,
    )

from temporalio.common import RetryPolicy

NO_RETRY = RetryPolicy(maximum_attempts=1)
LIGHT_RETRY = RetryPolicy(maximum_attempts=3)


@workflow.defn
class LDFSyncWorkflow:
    """Sync HDT files for all (or one) KGs into the shared LDF PVC and
    trigger a rolling restart of the LDF deployment.

    Per-KG:
      1. resolve_kg_ref → pick latest tag (fallback main), get commit id.
      2. Compare commit id to state ConfigMap.
      3. If changed: submit a k8s Job that pulls HDT into the PVC.
      4. Update state ConfigMap with new commit id (only after Job success).

    After all KGs are processed, render LDF datasource ConfigMap (registry +
    extras) and patch the deployment annotation to trigger a rolling update.
    """

    @workflow.run
    async def run(self, only_repo: str = None) -> dict:
        # 0. Ensure PVC exists
        await workflow.execute_activity(
            ensure_ldf_pvc,
            start_to_close_timeout=timedelta(minutes=2),
            retry_policy=LIGHT_RETRY,
        )

        # 1. Discover KGs
        all_kgs = await workflow.execute_activity(
            fetch_all_kgs,
            start_to_close_timeout=timedelta(minutes=1),
            retry_policy=LIGHT_RETRY,
        )
        if only_repo:
            kgs = [kg for kg in all_kgs if kg["repo"] == only_repo]
        else:
            kgs = all_kgs

        # 2. Read prior state
        state = await workflow.execute_activity(
            read_ldf_state,
            start_to_close_timeout=timedelta(minutes=1),
            retry_policy=LIGHT_RETRY,
        )

        synced = []
        skipped = []
        failed = []

        for kg in kgs:
            repo = kg["repo"]
            shortname = kg["shortname"]
            hdt_path = kg.get("hdt_path", "hdt")

            try:
                ref_info = await workflow.execute_activity(
                    resolve_kg_ref,
                    args=[repo],
                    start_to_close_timeout=timedelta(minutes=2),
                    retry_policy=LIGHT_RETRY,
                )
            except Exception as e:
                failed.append({"repo": repo, "stage": "resolve_ref", "error": str(e)})
                continue

            commit = ref_info["commit"]
            ref = ref_info["ref"]
            prior = state.get(repo)
            if prior == commit:
                skipped.append({"repo": repo, "commit": commit, "ref": ref})
                continue

            try:
                job_name = await workflow.execute_activity(
                    submit_ldf_sync_job,
                    args=[repo, ref, shortname, hdt_path],
                    start_to_close_timeout=timedelta(minutes=2),
                    retry_policy=NO_RETRY,
                )
                await workflow.execute_activity(
                    wait_ldf_sync_job,
                    args=[job_name, 7200],
                    start_to_close_timeout=timedelta(hours=2, minutes=10),
                    retry_policy=NO_RETRY,
                )
                state[repo] = commit
                synced.append({"repo": repo, "ref": ref, "commit": commit, "job": job_name})
                # Persist state after each KG so partial progress survives a worker crash.
                await workflow.execute_activity(
                    write_ldf_state,
                    args=[state],
                    start_to_close_timeout=timedelta(minutes=1),
                    retry_policy=LIGHT_RETRY,
                )
            except Exception as e:
                failed.append({"repo": repo, "stage": "sync", "error": str(e)})
                continue

        # 3. Render config + rolling restart (always, so newly registered KGs
        # appear in the datasource list even when no HDT changed).
        config_hash = await workflow.execute_activity(
            apply_ldf_config_and_rollout,
            start_to_close_timeout=timedelta(minutes=5),
            retry_policy=LIGHT_RETRY,
        )

        return {
            "synced": synced,
            "skipped": skipped,
            "failed": failed,
            "config_hash": config_hash,
        }
