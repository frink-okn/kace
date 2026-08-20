"""Federation server deployment workflow.

Mounts the index PVC produced by QLeverIndexWorkflow at `/index` in a
qlever-server Deployment exposed at `/federation`. Single-flight: webhook
cancels any prior workflow with id `qlever-federation-deploy` before starting
a new one.

Inputs:
  * use_previous (bool, default False) — mount build_id_previous instead of
    build_id_serving. Rollback path.
  * build_id (str, default None) — explicit override, wins over use_previous.

Outputs: {"build_id", "pvc", "deployment", "source"}.
"""
from datetime import timedelta
from temporalio import workflow
from temporalio.common import RetryPolicy

with workflow.unsafe.imports_passed_through():
    from ..activities import (
        resolve_qlever_federation_build_id,
        deploy_qlever_federation,
        create_qlever_index_pvc,
        submit_qlever_index_download,
        watch_k8s_job_sync,
        read_qlever_state,
        gc_qlever_index_pvcs,
        notify_slack,
    )


NO_RETRY = RetryPolicy(maximum_attempts=1)
QUICK_TIMEOUT  = timedelta(minutes=5)
DEPLOY_TIMEOUT = timedelta(minutes=30)
# Pulling the index out of the transfer bucket onto the serving PVC.
STAGE_TIMEOUT   = timedelta(days=1)
HEARTBEAT_TIMEOUT = timedelta(minutes=5)


@workflow.defn
class QLeverFederationDeploymentWorkflow:
    @workflow.run
    async def run(self, use_previous: bool = False, build_id: str = None) -> dict:
        resolved = await workflow.execute_activity(
            resolve_qlever_federation_build_id,
            args=[use_previous, build_id],
            start_to_close_timeout=QUICK_TIMEOUT,
            retry_policy=NO_RETRY,
        )

        await workflow.execute_activity(
            notify_slack,
            args=[
                f"🚀 Federated qlever-server rollover starting → build {resolved['build_id']} "
                f"(source: {resolved['source']}, pvc: `{resolved['pvc_name']}`)."
            ],
            start_to_close_timeout=QUICK_TIMEOUT,
            retry_policy=NO_RETRY,
        )

        # Stage the index onto the serving cluster before the server that mounts
        # it exists. Both steps no-op in single-cluster mode (the build PVC is
        # already the serving PVC), and the download job short-circuits on its
        # marker file, so a rollback to a build still on disk costs seconds.
        await workflow.execute_activity(
            create_qlever_index_pvc,
            args=[resolved["build_id"], resolved["image"], "remote"],
            start_to_close_timeout=QUICK_TIMEOUT,
            retry_policy=NO_RETRY,
        )
        stage_job = await workflow.execute_activity(
            submit_qlever_index_download,
            args=[resolved["build_id"], resolved["pvc_name"]],
            start_to_close_timeout=QUICK_TIMEOUT,
            retry_policy=NO_RETRY,
        )
        if stage_job:
            await workflow.execute_activity(
                watch_k8s_job_sync,
                args=[stage_job, 5, "remote"],
                start_to_close_timeout=STAGE_TIMEOUT,
                heartbeat_timeout=HEARTBEAT_TIMEOUT,
                retry_policy=NO_RETRY,
            )

        try:
            result = await workflow.execute_activity(
                deploy_qlever_federation,
                args=[resolved["build_id"], resolved["pvc_name"], resolved["image"]],
                start_to_close_timeout=DEPLOY_TIMEOUT,
                retry_policy=NO_RETRY,
            )
        except Exception as e:
            await workflow.execute_activity(
                notify_slack,
                args=[
                    f"❌ Federated qlever-server rollover failed (build {resolved['build_id']}): {e}"
                ],
                start_to_close_timeout=QUICK_TIMEOUT,
                retry_policy=NO_RETRY,
            )
            raise

        await workflow.execute_activity(
            notify_slack,
            args=[
                f"✅ Federated qlever-server serving build {result['build_id']} "
                f"at /{resolved['federation_prefix']}."
            ],
            start_to_close_timeout=QUICK_TIMEOUT,
            retry_policy=NO_RETRY,
        )

        # Serving PVCs are allocated on the remote cluster, so they are GC'd from
        # here (the build cluster's GC only sees its own).
        #
        # Only when we just deployed the *serving* build. GC keeps the serving and
        # previous builds and deletes the rest -- so running it after a rollback or
        # an explicit build_id would happily delete the very PVC the server has
        # just been pointed at.
        if resolved["source"] == "serving":
            state = await workflow.execute_activity(
                read_qlever_state,
                start_to_close_timeout=QUICK_TIMEOUT,
                retry_policy=NO_RETRY,
            )
            await workflow.execute_activity(
                gc_qlever_index_pvcs,
                args=[state, workflow.now().isoformat(), "remote"],
                start_to_close_timeout=QUICK_TIMEOUT,
                retry_policy=NO_RETRY,
            )

        return {**result, "source": resolved["source"]}
