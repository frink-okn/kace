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
        notify_slack,
    )


NO_RETRY = RetryPolicy(maximum_attempts=1)
QUICK_TIMEOUT  = timedelta(minutes=5)
DEPLOY_TIMEOUT = timedelta(minutes=30)


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

        return {**result, "source": resolved["source"]}
