import logging
import re
import shlex

import hashlib as _hashlib
import secrets as _secrets

import uvicorn
from fastapi import FastAPI, Query, Body, Depends, Header, HTTPException # Added Body
from models.lakefs_models import LakefsMergeActionModel, LakefTagCreationModel # Added LakefTagCreationModel
from models.kg_metadata import KGConfig, KG # Added KG
import memory_sizing
from config import config
from canary.slack import slack_canary
from temporal_app.client import get_client
from temporal_app.workflows.hdt_conversion import HDTConversionInput
from temporalio.client import WorkflowExecutionStatus

def require_token(x_kace_token: str = Header(None)) -> None:
    """Shared-secret gate on every endpoint.

    These endpoints are unauthenticated-by-default dangerous: they cancel running
    workflows, start multi-day index builds, roll the public /federation endpoint
    onto a caller-chosen build, and delete a repo's working files. LakeFS sends the
    token via `properties.headers` in the action yaml (see README); operators
    triggering by hand pass `-H "X-KACE-Token: ..."`.

    Fail closed: an unconfigured token serves nothing rather than everything.
    """
    if not config.webhook_token:
        raise HTTPException(status_code=503, detail="webhook auth not configured")

    # Distinguish absent from wrong. A caller that templates the token from its
    # own config (lakeFS does) can fail either way, and "invalid or missing"
    # sends you hunting the wrong half: absent means the header was dropped in
    # transit or never set, wrong means substitution produced something else.
    # Absent and empty are different failures and must not be conflated: an
    # empty value is what a caller sends when its own templating resolved to
    # nothing. lakeFS does exactly this when actions.env.enabled is false --
    # it substitutes "", marks it secret, and prints ***** in its run log, so
    # from the sender's side it looks like the token went out fine.
    if x_kace_token is None:
        logger.warning("webhook rejected: no X-KACE-Token header on the request")
        raise HTTPException(status_code=401, detail="missing X-KACE-Token header")

    if x_kace_token == "":
        logger.warning(
            "webhook rejected: X-KACE-Token present but empty -- the sender's "
            "variable substitution produced nothing"
        )
        raise HTTPException(status_code=401, detail="empty X-KACE-Token header")

    if not _secrets.compare_digest(x_kace_token, config.webhook_token):
        # Fingerprint only -- enough to compare against the expected value
        # without writing either secret to a log.
        digest = _hashlib.sha256(x_kace_token.encode()).hexdigest()[:8]
        shape = (
            "looks-like-unsubstituted-template" if "{{" in x_kace_token
            else "all-asterisks" if set(x_kace_token) == {"*"}
            else "opaque"
        )
        logger.warning(
            f"webhook rejected: X-KACE-Token mismatch (len={len(x_kace_token)} "
            f"sha256={digest} shape={shape}; expected len={len(config.webhook_token)} "
            f"sha256={_hashlib.sha256(config.webhook_token.encode()).hexdigest()[:8]})"
        )
        raise HTTPException(status_code=401, detail=f"invalid X-KACE-Token (len={len(x_kace_token)})")


app = FastAPI(
    title="KACE Temporal Webhook Server",
    description="Webhook receiver that triggers Temporal workflows for HDT and Neo4j conversions. "
                "Enforces at-most-one-workflow-per-repo by cancelling any existing workflow before starting a new one.",
    dependencies=[Depends(require_token)],
)

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

async def cancel_existing_workflow(client, workflow_id: str) -> bool:
    """
    Attempt to cancel a running workflow by its ID.
    Returns True if a workflow was cancelled, False otherwise.
    """
    try:
        handle = client.get_workflow_handle(workflow_id)
        desc = await handle.describe()
        if desc.status in (WorkflowExecutionStatus.RUNNING, WorkflowExecutionStatus.CONTINUED_AS_NEW):
            logger.info(f"Cancelling existing workflow {workflow_id} (status={desc.status.name})")
            await handle.cancel()
            return True
    except Exception as e:
        # Workflow not found or already completed – nothing to cancel
        logger.debug(f"No active workflow to cancel for {workflow_id}: {e}")
    return False


async def cleanup_repo_files(repo_id: str) -> None:
    """
    Remove local working directory for the repo so stale files from a
    prior (possibly cancelled) run don't interfere.
    """
    from lakefs_util.io_util import clean_up_files
    logger.info(f"Cleaning up local files for {repo_id}")
    clean_up_files(repo_id)


# ---------------------------------------------------------------------------
# Endpoints
# ---------------------------------------------------------------------------

@app.post("/convert_to_hdt")
async def convert_to_hdt(
    action_model: LakefsMergeActionModel,
    cpu: int = Query(3),
    memory: str = Query("4Gi"),
    ephemeral: str = Query("256Mi"),
    jvm_flags: str = Query(
        memory_sizing.JVM_DEFAULT_FLAGS,
        description="Non-memory JVM flags for the qendpoint jobs; -Xmx/-Xms are "
                    "derived from `memory`.",
    ),
    hdt_exists: bool = Query(False),
    hdt_path: str = Query("hdt/"),
    exclude_files: str = Query(""),
    exclude_known_extension: str = Query(""),
):
    """
    Trigger the HDTConversionWorkflow via Temporal.
    Mirrors the existing server.py /convert_to_hdt endpoint signature.
    Enforces at most one HDT workflow per repo.
    """
    repo_id = action_model.repository_id
    workflow_id = f"hdt-{repo_id}"

    # Read KG config
    try:
        kg_config = (await KGConfig.from_git()).get_by_repo(repo_id=repo_id)
    except Exception as e:
        slack_canary.send_slack_message(
            f"⚠️ Failed to read registry config from {config.kg_config_url}, {str(e)}"
        )
        raise e

    # Pod memory is the only memory input an action gives. Everything the jobs
    # need is derived from it and passed into the workflow, so the Temporal UI
    # shows the numbers a run actually used. See src/memory_sizing.py.
    mem_size = memory_sizing.hdtc_budget(memory)
    stxxl_memory = memory_sizing.stxxl_budget(memory)
    java_opts = memory_sizing.jvm_opts(memory, jvm_flags)
    logger.info(
        f"{repo_id}: pod {memory} -> hdtc {mem_size}, stxxl {stxxl_memory}, "
        f"java_opts '{java_opts}'"
    )
    for warning in memory_sizing.sizing_warnings(memory, mem_size, java_opts):
        logger.warning(f"{repo_id}: {warning}")
    slack_canary.notify_event(
        event_name="Merge to Main (Temporal)",
        repository=repo_id,
        commit=action_model.commit_id,
        branch=action_model.branch_id,
    )

    # Cancel any in-flight workflow for this repo and clean up files
    client = await get_client()
    cancelled = await cancel_existing_workflow(client, workflow_id)
    if cancelled:
        slack_canary.send_message(
            f"🔄 Cancelled previous HDT workflow for {repo_id}, starting fresh."
        )

    # Clean up local files before starting
    await cleanup_repo_files(repo_id)

    # Parse exclude_files
    parsed_exclude_files = [x.strip() for x in exclude_files.split(",") if x.strip()] if exclude_files else None
    parsed_exclude_known_extension = [x.strip().lstrip(".") for x in exclude_known_extension.split(",") if x.strip()] if exclude_known_extension else None

    # Start the workflow
    handle = await client.start_workflow(
        "HDTConversionWorkflow",
        HDTConversionInput(
            action_payload=action_model.dict(),
            doc_path=kg_config.frink_options.documentation_path,
            cpu=cpu,
            pod_memory=memory,
            ephemeral=ephemeral,
            java_opts=java_opts,
            program_memory=mem_size,
            stxxl_memory=stxxl_memory or "",
            convert_to_hdt=not hdt_exists,
            hdt_path=hdt_path,
            exclude_files=parsed_exclude_files,
            exclude_known_extension=parsed_exclude_known_extension
        ),
        id=workflow_id,
        task_queue="frink-temporal-queue",
    )

    logger.info(f"Started HDTConversionWorkflow: id={handle.id}, run_id={handle.result_run_id}")
    return {
        "message": "Started HDT conversion workflow via Temporal.",
        "workflow_id": handle.id,
        "run_id": handle.result_run_id,
    }


@app.post("/convert_neo4j_to_hdt")
async def convert_neo4j_to_hdt(
    action_model: LakefsMergeActionModel,
    cpu: int = Query(1),
    memory: str = Query("28Gi"),
    ephemeral: str = Query("512Mi"),
    jvm_flags: str = Query(
        memory_sizing.JVM_DEFAULT_FLAGS,
        description="Non-memory JVM flags; -Xmx/-Xms are derived from `memory`.",
    ),
):
    """
    Trigger the Neo4jConversionWorkflow via Temporal.
    Mirrors the existing server.py /convert_neo4j_to_hdt endpoint signature.
    Enforces at most one Neo4j workflow per repo.
    """
    repo_id = action_model.repository_id
    workflow_id = f"neo4j-{repo_id}"

    # Same derivation as /convert_to_hdt -- this path used to hardcode 25G of a
    # 28Gi pod (89%) all the way through to the child HDT workflow.
    mem_size = memory_sizing.hdtc_budget(memory)
    stxxl_memory = memory_sizing.stxxl_budget(memory)
    java_opts = memory_sizing.jvm_opts(memory, jvm_flags)
    logger.info(
        f"{repo_id}: pod {memory} -> hdtc {mem_size}, stxxl {stxxl_memory}, "
        f"java_opts '{java_opts}'"
    )
    for warning in memory_sizing.sizing_warnings(memory, mem_size, java_opts):
        logger.warning(f"{repo_id}: {warning}")

    slack_canary.notify_event(
        event_name="New data upload on Neo4j based repo. Conversion starting (Temporal)",
        repository=repo_id,
        commit=action_model.commit_id,
        branch=action_model.branch_id,
    )

    # Cancel any in-flight workflow for this repo and clean up files
    client = await get_client()
    cancelled = await cancel_existing_workflow(client, workflow_id)
    if cancelled:
        slack_canary.send_message(
            f"🔄 Cancelled previous Neo4j workflow for {repo_id}, starting fresh."
        )

    # Clean up local files before starting
    await cleanup_repo_files(repo_id)

    # Start the workflow
    handle = await client.start_workflow(
        "Neo4jConversionWorkflow",
        args=[
            action_model.dict(),
            None,          # rdf_mapping_config
            cpu,
            memory,
            ephemeral,
            java_opts,
            mem_size,
            stxxl_memory,
        ],
        id=workflow_id,
        task_queue="frink-temporal-queue",
    )

    logger.info(f"Started Neo4jConversionWorkflow: id={handle.id}, run_id={handle.result_run_id}")
    return {
        "message": "Started Neo4j conversion workflow via Temporal.",
        "workflow_id": handle.id,
        "run_id": handle.result_run_id,
    }


@app.post("/handle_tag_creation")
async def handle_tag_creation(
    cpu: str = Query(None),
    pod_memory: str = Query(None, description="K8s pod memory request/limit (e.g. '8Gi')"),
    mem_size: str = Query(None),
    hdt_path: str = Query('hdt/'),
    action_model: LakefTagCreationModel = Body(...),
    kg_name: str = Query(None),
    qlever_args: str = Query(None, description="Extra qlever-server args appended after '-i ... -p 7001' (e.g. '-j 16 -m 5734M -c 5734M -e 1433M')"),
):
    """
    Trigger the QLeverDeploymentWorkflow via Temporal.
    Enforces at most one deployment workflow per repo.
    """
    repo_id = action_model.repository_id
    workflow_id = f"qlever-deployment-{repo_id}"
    if mem_size:
        mem_size = mem_size.replace('i', '')
    parsed_qlever_args = shlex.split(qlever_args) if qlever_args else None
    # Resolve kg_config and kg_name

    try:
        kg_config_obj = (await KGConfig.from_git()).get_by_repo(repo_id=repo_id)
        if not kg_config_obj:
            raise Exception(f"No KG configuration found for repository: {repo_id}")
        kg_name = kg_config_obj.shortname
        kg_config_dict = kg_config_obj.dict()
    except Exception as e:
        slack_canary.send_slack_message(
            f"⚠️ Failed to read registry config from {config.kg_config_url}, {str(e)}"
        )
        raise e

    slack_canary.notify_event(
        event_name="Tag is created, proceeding to QLever deployment (Temporal)",
        repository=repo_id,
        tag_created=action_model.tag_id,
        external_spaql_address=f"{config.frink_address}/{kg_name}/sparql"
    )

    # Cancel any in-flight workflow for this repo
    client = await get_client()
    cancelled = await cancel_existing_workflow(client, workflow_id)
    if cancelled:
        slack_canary.send_message(
            f"🔄 Cancelled previous QLever deployment workflow for {repo_id}, starting fresh."
        )

    # Start the workflow
    handle = await client.start_workflow(
        "QLeverDeploymentWorkflow",
        args=[
            kg_config_dict,
            cpu,
            pod_memory,
            mem_size,
            action_model.dict(),
            parsed_qlever_args,
        ],
        id=workflow_id,
        task_queue="frink-temporal-queue",
    )

    logger.info(f"Started QLeverDeploymentWorkflow: id={handle.id}, run_id={handle.result_run_id}")

    # Also kick off LDF sync for this single repo so the aggregator picks up
    # the new tag without needing a manual /sync_ldf call.
    ldf_workflow_id = f"ldf-sync-{repo_id}"
    try:
        await cancel_existing_workflow(client, ldf_workflow_id)
        ldf_handle = await client.start_workflow(
            "LDFSyncWorkflow",
            args=[repo_id],
            id=ldf_workflow_id,
            task_queue="frink-temporal-queue",
        )
        logger.info(f"Started LDFSyncWorkflow: id={ldf_handle.id}")
    except Exception as e:
        logger.warning(f"Could not start LDFSyncWorkflow for {repo_id}: {e}")

    return {
        "message": f"Started QLever deployment via Temporal, anticipated address {config.frink_address}/{kg_name}/sparql",
        "workflow_id": handle.id,
        "run_id": handle.result_run_id,
    }


@app.post("/sync_ldf")
async def sync_ldf(only_repo: str = Query(None, description="Optional lakefs repo to sync; omit to sync all KGs")):
    """Trigger a full (or single-repo) LDF aggregator sync."""
    workflow_id = f"ldf-sync-{only_repo or 'all'}"
    client = await get_client()
    await cancel_existing_workflow(client, workflow_id)
    handle = await client.start_workflow(
        "LDFSyncWorkflow",
        args=[only_repo],
        id=workflow_id,
        task_queue="frink-temporal-queue",
    )
    return {
        "message": "Started LDF sync workflow.",
        "workflow_id": handle.id,
        "run_id": handle.result_run_id,
        "scope": only_repo or "all",
    }


@app.post("/trigger_qlever_index")
async def trigger_qlever_index(only_kg: list = Body(None,
        description="Optional list of kg shortnames to restrict the build to. "
                    "Omit/null for a full federated rebuild.")):
    """Kick off the federated QLever index build.

    Enforces single-flight via the deterministic workflow id `qlever-index-build`.
    Any in-flight build is cancelled before a new one starts."""
    workflow_id = "qlever-index-build"
    client = await get_client()
    cancelled = await cancel_existing_workflow(client, workflow_id)
    if cancelled:
        slack_canary.send_message(
            "🔄 Cancelled in-flight QLever index build to start a new one."
        )
    handle = await client.start_workflow(
        "QLeverIndexWorkflow",
        args=[only_kg],
        id=workflow_id,
        task_queue="frink-temporal-queue",
    )
    return {
        "message":     "Started QLever federated index build.",
        "workflow_id": handle.id,
        "run_id":      handle.result_run_id,
        "only_kg":     only_kg,
    }


@app.post("/trigger_qlever_federation_deploy")
async def trigger_qlever_federation_deploy(
    use_previous: bool = Body(False,
        description="If true, mount the previous build PVC (build_id_previous) "
                    "instead of the currently-serving one. Rollback path."),
    build_id: str = Body(None,
        description="Explicit build_id override. Wins over use_previous. "
                    "Useful for pinning a specific historical PVC."),
):
    """Roll over the federated qlever-server (`/federation`) to a build PVC.

    Default: serve the build_id currently marked `serving` in
    `kace-qlever-state`. Pass `use_previous=true` for n-1, or `build_id=...`
    for an explicit pin. Single-flight via workflow id
    `qlever-federation-deploy`.
    """
    workflow_id = "qlever-federation-deploy"
    client = await get_client()
    cancelled = await cancel_existing_workflow(client, workflow_id)
    if cancelled:
        slack_canary.send_message(
            "🔄 Cancelled in-flight federation deploy to start a new one."
        )
    handle = await client.start_workflow(
        "QLeverFederationDeploymentWorkflow",
        args=[use_previous, build_id],
        id=workflow_id,
        task_queue="frink-temporal-queue",
    )
    return {
        "message":     "Started federated QLever server deployment.",
        "workflow_id": handle.id,
        "run_id":      handle.result_run_id,
        "use_previous": use_previous,
        "build_id":    build_id,
    }


# Serve the whole app under a path prefix when asked (WEBHOOK_PATH_PREFIX=/kace
# → POST /kace/convert_to_hdt). Mounting a sub-app makes the prefix the app's
# own business: no ingress rewrite-target, no use-regex, nothing for an
# admission webhook to object to. The mounted app keeps its own dependencies,
# so require_token still guards every route.
if config.webhook_path_prefix:
    _root = FastAPI()
    _root.mount(config.webhook_path_prefix, app)
    logger.info(f"Serving webhook endpoints under {config.webhook_path_prefix}")
    app = _root

if __name__ == "__main__":
    uvicorn.run(app, host="0.0.0.0", port=9899)
