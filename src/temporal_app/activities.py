from temporalio import activity
from k8s.podman import JobMan
from k8s import fuseki_server_manager, ldf_server_manager
from lakefs_util.io_util import resolve_commit, download_file_from_latest_tag, download_files, upload_files, clean_up_files, resolve_future_tag, get_lakefs_prefix_size, download_hdt_files, get_latest_commit, get_latest_tag, download_file_at_ref, object_exists
from canary.slack import slack_canary
from canary.mail import mail_canary
from models.lakefs_models import LakefsMergeActionModel, LakefTagCreationModel
from models.kg_metadata import KGConfig, KG
from config import config
import asyncio
import os
import re
from log_util import LoggingUtil
from config import config as app_config

logger = LoggingUtil.init_logging(__name__)


def _memory_str_to_mib(s: str) -> int:
    """Parse k8s/QLever memory string (e.g. '8Gi', '4096M') to integer MiB."""
    m = re.fullmatch(r'(\d+(?:\.\d+)?)\s*(Gi?|Mi?|Ki?|Ti?)?', str(s).strip(), re.IGNORECASE)
    if not m:
        raise ValueError(f"Cannot parse memory string: {s!r}")
    val = float(m.group(1))
    unit = (m.group(2) or 'M').upper().rstrip('I')
    return int(val * {'G': 1024, 'M': 1, 'K': 1 / 1024, 'T': 1024 * 1024}.get(unit, 1))

@activity.defn
async def run_k8s_job(job_type: str, job_name: str, repo: str, branch: str,
                      command: list = None, args: list = None,
                      resources: dict = None, env_vars: dict = None,
                      additional_volume_mounts: list = None,
                      image: str = None, extra_pvcs: list = None,
                      read_only_default_mount: bool = False,
                      pod_security_context: dict = None) -> str:
    logger.info(f"Starting K8s job: {job_name} ({job_type})")
    # Inject GH_TOKEN from config if not explicitly provided.
    # This keeps secrets out of Temporal workflow history.
    if env_vars is None:
        env_vars = {}
    if "GH_TOKEN" not in env_vars and config.gh_token:
        env_vars["GH_TOKEN"] = config.gh_token
    job_man = JobMan()
    job_man.run_job(
        job_type=job_type,
        job_name=job_name,
        repo=repo,
        branch=branch,
        command=command,
        args=args,
        resources=resources,
        env_vars=env_vars,
        additional_volume_mounts=additional_volume_mounts,
        image=image,
        extra_pvcs=extra_pvcs,
        read_only_default_mount=read_only_default_mount,
        pod_security_context=pod_security_context,
    )
    return job_name

@activity.defn
async def watch_k8s_job_sync(job_name: str, poll_interval: int = 5) -> None:
    """Poll a K8s Job until terminal. Heartbeats Temporal every poll so the
    activity can outlive worker restarts (paired with `heartbeat_timeout` on
    the workflow side) and won't be killed by start_to_close_timeout when the
    underlying Job legitimately runs for days."""
    from kubernetes import client as k8s_client
    import asyncio as _asyncio
    logger.info(f"Watching K8s job: {job_name}")
    job_man = JobMan()
    batch_v1 = k8s_client.BatchV1Api()
    while True:
        try:
            job = batch_v1.read_namespaced_job(name=job_name, namespace=job_man.namespace)
        except k8s_client.exceptions.ApiException as e:
            raise Exception(f"Failed to fetch Job '{job_name}': {e}, likely the job was never created.")
        succeeded     = job.status.succeeded or 0
        failed        = job.status.failed or 0
        backoff_limit = job.spec.backoff_limit if job.spec.backoff_limit is not None else 6
        activity.heartbeat({
            "job_name":  job_name,
            "succeeded": succeeded,
            "failed":    failed,
        })
        if succeeded > 0:
            logger.info(f"Job '{job_name}' completed successfully.")
            return
        if failed > backoff_limit:
            pod_info = job_man._get_pod_logs_for_job(job_name)
            raise Exception(f"Job '{job_name}' failed after {failed} attempts.\n{pod_info}")
        await _asyncio.sleep(poll_interval)


@activity.defn
async def deploy_fuseki(kg_config: dict, lakefs_action: dict, cpu: str = None, memory: str = "28Gi") -> None:
    kg_config_obj = KG(**kg_config)
    lakefs_action_obj = LakefTagCreationModel(**lakefs_action)
    
    kg_name = kg_config_obj.shortname
    annotations = {
        "kg-name": kg_name,
        "version": lakefs_action_obj.tag_id,
        "lakefs-repository": lakefs_action_obj.repository_id,
        "commit": lakefs_action_obj.commit_id
    }
    resources = {
        "requests": {"memory": memory},
        "limits": {"memory": memory}
    }
    if cpu:
        resources["limits"]["cpu"] = cpu

    logger.info(f"Deploying Fuseki for {kg_name}")
    fuseki_server_manager.create_all(
        parameters={"kg_name": kg_name},
        annotations=annotations,
        resources=resources
    )

    retries = 10
    # ensure wait_for_services is called appropriately (it sleeps/retries)
    server_up = fuseki_server_manager.wait_for_services_to_be_running(
        parameters={"kg_name": kg_name},
        annotations=annotations,
        max_retries=retries
    )
    if not server_up:
        raise Exception(f"Fuseki deployment for {kg_name} failed check.")

@activity.defn
async def deploy_ldf(kg_config: dict, lakefs_action: dict) -> None:
    kg_config_obj = KG(**kg_config)
    lakefs_action_obj = LakefTagCreationModel(**lakefs_action)
    kg_name = kg_config_obj.shortname
    
    annotations = {
        "kg-name": kg_name,
        "version": lakefs_action_obj.tag_id,
        "lakefs-repository": lakefs_action_obj.repository_id,
        "commit": lakefs_action_obj.commit_id
    }

    logger.info(f"Deploying LDF for {kg_name}")
    ldf_server_manager.create_all(
        parameters={
            "kg_name": kg_name,
            "host_name": config.frink_address,
        },
        annotations=annotations,
        resources=None
    )


@activity.defn
async def deploy_qlever(kg_config: dict, lakefs_action: dict, cpu: str = "1", memory: str = "2G", mem_size: str = None, pvc_storage_size: str = "2Gi", qlever_args: list = None) -> None:
    from k8s import qlever_server_manager
    kg_config_obj = KG(**kg_config)
    lakefs_action_obj = LakefTagCreationModel(**lakefs_action)
    
    kg_name = kg_config_obj.shortname
    
    version = lakefs_action_obj.tag_id
    if not version and lakefs_action_obj.commit_id:
        version = lakefs_action_obj.commit_id[:7]
    
    # We sanitize the version to be DNS-1123 compatible (used in PVC naming)
    sanitized_version = str(version).replace('.', '-').replace('_', '-')
    
    annotations = {
        "kg-name": kg_name,
        "version": version,
        "lakefs-repository": lakefs_action_obj.repository_id,
        "commit": lakefs_action_obj.commit_id
    }
    resources = {
        "requests": {"memory": memory},
        "limits": {"memory": memory}
    }
    if cpu:
        resources["limits"]["cpu"] = cpu
        resources["requests"]["cpu"] = cpu

    logger.info(f"Deploying QLever for {kg_name} version {version}")
    
    # Ensure standard env mappings for the template:
    mem_size = mem_size or "10G"

    if not qlever_args:
        # -c ≈ 70% of pod memory limit
        # -e (per-entry cache cap) = -c / 4 so 4+ large results can coexist
        #    without any single one evicting everything else
        total_mib = _memory_str_to_mib(memory)
        cache_mib = int(total_mib * 0.70)
        entry_mib = cache_mib // 4
        qlever_args = ["-j", "8", "-m", f"{cache_mib}M", "-c", f"{cache_mib}M", "-e", f"{entry_mib}M"]

    parameters = {
        "kg_name": kg_name,
        "version": version,
        "pvc_name": f"frink-{kg_name}-qlever-pvc-{sanitized_version}",
        "lakefs_url": config.lakefs_url.replace("https://", "s3://").replace("http://", "s3://"),  # s5cmd s3 url
        "lakefs_access_key": config.lakefs_access_key,
        "lakefs_secret_key": config.lakefs_secret_key,
        "repository_id": lakefs_action_obj.repository_id,
        "branch_id": lakefs_action_obj.tag_id if lakefs_action_obj.tag_id else lakefs_action_obj.commit_id,
        "host_name": config.frink_address,
        "pvc_storage_size": pvc_storage_size,
        "qlever_storage_class": config.qlever_storage_class,
        "mem_size": mem_size,
        "cpu": cpu,
        "memory": memory,
        "qlever_args": qlever_args or [],
    }
    
    # Needs a real S3 endpoint URL for s5cmd: 
    # Example: if lakefs_url is https://frink-lakefs.apps.renci.org
    parameters['lakefs_url'] = config.lakefs_url
    
    qlever_server_manager.create_all(
        parameters=parameters,
        annotations=annotations,
        resources=resources
    )

    retries = 10
    # ensure wait_for_services is called appropriately (it sleeps/retries)
    server_up = qlever_server_manager.wait_for_services_to_be_running(
        parameters=parameters,
        annotations=annotations,
        max_retries=retries
    )
    
    if not server_up:
        raise Exception(f"QLever deployment for {kg_name} failed check.")
        
    logger.info(f"QLever deployment verified healthy for {kg_name}. Pruning older deployments...")
    qlever_server_manager.prune_old_deployments(kg_name=kg_name, keep_version=version)

@activity.defn
async def notify_slack(message: str, channel: str = None) -> None:
    # Uses slack_canary global instance
    logger.info(f"Sending Slack message: {message}")
    slack_canary.send_message(message)

@activity.defn
async def notify_email_deployed(kg_name: str, version: str, recipient_email: str) -> None:
    logger.info(f"Sending deployment email for {kg_name} {version} to {recipient_email}")
    mail_canary.send_deployed_email(
        kg_name=kg_name,
        version=version,
        recipient_email=recipient_email
    )

@activity.defn
async def resolve_commit_details(repo: str, commit_id: str) -> dict:
    # resolve_commit returns an object, we'll convert to dict for safety
    commit_data = resolve_commit(repo, commit_id)
    return {
        "committer": commit_data.committer,
        "message": commit_data.message,
        "id": commit_data.id,
        "creation_date": commit_data.creation_date.isoformat() if commit_data.creation_date else None
    }

@activity.defn
async def download_hdt_files_activity(repo: str, branch: str, kg_name: str, hdt_path: str = 'hdt') -> None:
    logger.info(f"Downloading HDT files for {kg_name} from {repo}@{branch}")
    await download_hdt_files(repo, branch, kg_name, hdt_path)

@activity.defn
async def download_file_lakefs(repo: str, remote_path: str, local_path: str, ref: str = None) -> None:
    # this function in io_util is async
    if ref:
        await download_file_at_ref(repo, ref, remote_path, local_path)
    else:
        await download_file_from_latest_tag(repo, remote_path, local_path)
    # Resolve-time pre-flight already filtered out missing sources, so a
    # zero-byte file here means an in-flight transport failure rather than
    # an absent object. Treat as fatal — the build needs a real file.
    if not os.path.exists(local_path) or os.path.getsize(local_path) == 0:
        raise Exception(
            f"Download produced empty/no file at {local_path} for "
            f"{repo}@{ref or 'latest-tag'}:{remote_path}. Network/transport error suspected."
        )


@activity.defn
async def resolve_latest_tag(repo: str) -> str:
    """Return the highest semver tag for `repo`. Raises if repo has no tags."""
    tag = await get_latest_tag(repo)
    if not tag:
        raise Exception(f"No tags found on lakefs repo '{repo}'")
    return tag

@activity.defn
async def get_kg_config_from_git(repo_id: str) -> dict:
    # logic from celery.py: kg_config = asyncio.run(KGConfig.from_git()).get_by_repo(lakefs_payload.repository_id)
    # KGConfig.from_git() is async
    kg_config_full = await KGConfig.from_git()
    kg_config = kg_config_full.get_by_repo(repo_id)
    return kg_config.dict()

@activity.defn
async def download_input_files(repo: str, branch: str, extensions: list = None,
                               exclude_files: list = None,
                               exclude_known_extension: list = None,
                               delete_all_files: bool = True,
                               exclude_prefixes: list = None) -> list[str]:
    """Download input files from LakeFS. Returns list of downloaded file paths."""
    logger.info(f"Downloading input files from {repo}@{branch}")
    files = await download_files(
        repo=repo,
        branch=branch,
        extensions=extensions,
        exclude_files=exclude_files,
        exclude_known_extension=exclude_known_extension,
        delete_all_files=delete_all_files,
        exclude_prefixes=exclude_prefixes
    )
    logger.info(f"Downloaded {len(files)} files from {repo}@{branch}")
    return files

@activity.defn
async def upload_output_files(repo: str, root_branch: str,
                              local_files: list[list[str]]) -> dict:
    """Upload output files to LakeFS. Creates a stable branch and commits.
    
    Args:
        repo: LakeFS repository name
        root_branch: Branch to create stable branch from
        local_files: List of [local_path, remote_dir] pairs
    
    Returns:
        {stable_branch_name: str, future_tag: str}
    """
    logger.info(f"Uploading {len(local_files)} files to {repo}@{root_branch}")
    # Convert list-of-lists back to list-of-tuples (JSON serialization)
    file_tuples = [(f[0], f[1]) for f in local_files]
    result = await upload_files(
        repo=repo,
        root_branch=root_branch,
        local_files=file_tuples
    )
    logger.info(f"Uploaded to branch={result['stable_branch_name']}, tag={result['future_tag']}")
    return result

@activity.defn
async def cleanup_local_files(repo: str) -> None:
    """Remove local working directory for a repo after upload."""
    logger.info(f"Cleaning up local files for {repo}")
    clean_up_files(repo)

@activity.defn
async def send_review_email(recipient_email: str, branch_name: str, version: str,
                            repository_name: str, github_pr: str = "",
                            github_branch: str = "") -> None:
    """Send a review email after conversion is complete."""
    logger.info(f"Sending review email for {repository_name} to {recipient_email}")
    mail_canary.send_review_email(
        recipient_email=recipient_email,
        branch_name=branch_name,
        version=version,
        repository_name=repository_name,
        github_pr=github_pr,
        github_branch=github_branch
    )

# s2-builds lakefs repo: extra graphs that ride along with each qlever-index rebuild.
# All files live at the root of the s2-builds repo and are pinned to the highest
# semver tag at workflow start.
S2_LAKEFS_REPO = "s2-builds"
S2_GRAPHS = [
    # (filename in s2-builds, base kg shortname for only_kg filtering, target graph IRI)
    ("sockg-geo.nt.gz",         "sockg",      "https://purl.org/okn/frink/kg/sockg#s2"),
    ("hydrologykg-geo.nt.gz",   "hydrologykg","https://purl.org/okn/frink/kg/hydrologykg#s2"),
    ("ufokn-geo.nt.gz",         "ufokn",      "https://purl.org/okn/frink/kg/ufokn#s2"),
    ("spatialkg-geo.nt.gz",     "spatialkg",  "https://purl.org/okn/frink/kg/spatialkg#s2"),
    ("geoconnex-geo.nt.gz",     "geoconnex",  "https://purl.org/okn/frink/kg/geoconnex#s2"),
    ("sudokn-geosparql.nt.gz",  "sudokn",     "https://purl.org/okn/frink/kg/sudokn#geosparql"),
]

# Drops triples that declare a decimal value (e.g. "311119.0") as xsd:integer.
# qlever's IndexBuilderMain refuses to parse these. The SUDOKN NAICS data has
# them, and any KG that ingests SUDOKN nodes (e.g. secure-chain) inherits them.
_DROP_DECIMAL_AS_INTEGER = r"""| grep -Ev '\.[0-9]+"\^\^<http://www\.w3\.org/2001/XMLSchema#integer>'"""

# Per-shortname pipe suffix appended after `gunzip -c <file>` in the build command.
# Required to match the reference qlever-index invocation.
PER_REPO_INPUT_FILTERS = {
    "spatialkg":          '| grep -v "<http://stko-kwg.geog.ucsb.edu/lod/ontology/cellID>"',
    # "biobricks-aopwiki":  '| tail -n +28',
    # "biobricks-mesh":     '| tail -n +28',
    "sudokn":             _DROP_DECIMAL_AS_INTEGER,
    "securechainkg":      _DROP_DECIMAL_AS_INTEGER,
}

# Overrides for where the source nt file lives in lakefs. Keys may be either
# a kg shortname OR a lakefs repo name — the resolver checks both. Default
# behavior (no override): remote_path="nt/graph.nt.gz", ref=latest semver tag.
# wikidata: file at repo root and we always pull from main (no semver tags).
PER_REPO_LAKEFS_OVERRIDES = {
    "wikidata": {"remote_path": "graph.nt.gz", "ref": "main"},
}


@activity.defn
async def prepare_qlever_job_specs(kg_refs: dict, s2_tag: str, only_kg: list = None) -> dict:
    """Build the qlever IndexBuilderMain command + download manifest.

    Refs and tags are resolved by the workflow (resolve_qlever_refs) and
    passed in so every artifact in this build is pinned to a single, known
    snapshot. This function does not hit the network.

    Args:
        kg_refs: {repo_id: {"shortname", "ref", "commit", "remote_path"}}
                 — produced by resolve_qlever_refs.
        s2_tag:  semver tag of the s2-builds repo (e.g. "v0.0.1").
                 Required (s2 graphs always participate).
        only_kg: optional subset of kg shortnames to keep (already applied in
                 kg_refs; passed in only to filter the s2 block).

    Returns:
      - downloads:     [{repo, remote_path, local_path, ref}]
      - build_command: shell string for IndexBuilderMain (writes to /index/)
      - stxxl_memory:  string for --stxxl-memory (already part of build_command)
    """
    source_root  = app_config.qlever_source_path                  # /shared/qlever-source
    s2_local_dir = f"{source_root}/s2"
    index_dir    = "/index"                                       # mount of per-build output PVC
    stxxl_memory = app_config.qlever_indexer_stxxl_memory

    downloads = []
    for repo, meta in kg_refs.items():
        downloads.append({
            "repo":        repo,
            "remote_path": meta["remote_path"],
            "local_path":  f"{source_root}/{repo}/graph.nt.gz",
            "ref":         meta["ref"],
        })

    for fname, base_shortname, _iri in S2_GRAPHS:
        if only_kg and base_shortname not in only_kg:
            continue
        downloads.append({
            "repo":        S2_LAKEFS_REPO,
            "remote_path": fname,
            "local_path":  f"{s2_local_dir}/{fname}",
            "ref":         s2_tag,
        })

    build_cmd_parts = [
        f'ulimit -Sn 1048576; mkdir -p {index_dir} && cd {index_dir} && '
        'IndexBuilderMain -i frink -s /qlever/frink-qlever.settings.json'
    ]

    for repo, meta in kg_refs.items():
        shortname = meta["shortname"]
        file_path = f'{source_root}/{repo}/graph.nt.gz'
        pipe_filter = PER_REPO_INPUT_FILTERS.get(shortname, '')
        gunzip_body = f'gunzip -c {file_path}'
        if pipe_filter:
            gunzip_body = f'{gunzip_body} {pipe_filter}'
        build_cmd_parts.append(
            f'-f <({gunzip_body}) -g https://purl.org/okn/frink/kg/{shortname} -F nt'
        )

    for fname, base_shortname, iri in S2_GRAPHS:
        if only_kg and base_shortname not in only_kg:
            continue
        build_cmd_parts.append(
            f'-f <(gunzip -c {s2_local_dir}/{fname}) -g {iri} -F nt'
        )

    build_cmd_parts.append(f'--stxxl-memory {stxxl_memory}')
    return {
        "downloads": downloads,
        "build_command": ' '.join(build_cmd_parts),
        "stxxl_memory": stxxl_memory,
    }

@activity.defn
async def get_spider_config() -> dict:
    return {
        "ip": config.spider_ip,
        "port": config.spider_port
    }

@activity.defn
async def create_local_dir(dir_path: str, chmod_777: bool = False) -> None:
    """Create a local directory on the worker if it does not exist."""
    logger.info(f"Creating local directory: {dir_path}")
    os.makedirs(dir_path, exist_ok=True)
    if chmod_777:
        os.chmod(dir_path, 0o777)


@activity.defn
async def create_local_file(file_path: str, content: str, append=False) -> None:
    """Create a local file on the worker with the given content."""
    logger.info(f"Creating local file: {file_path}")
    if not append:
        with open(file_path, "w") as f:
            f.write(content)
    else:
        with open(file_path, "a") as f:
            f.write(content)


@activity.defn
async def get_qlever_index_files(qlever_location: str) -> list[list[str]]:
    """Get the generated Qlever index files from the local directory."""
    logger.info(f"Getting Qlever files from: {qlever_location}")
    result = []
    if os.path.exists(qlever_location):
        for f in os.listdir(qlever_location):
            if os.path.isfile(os.path.join(qlever_location, f)):
                result.append([os.path.join(qlever_location, f), "qlever"])
    return result

@activity.defn
async def get_future_tag(repo: str) -> str:
    """Get the next version tag for a repository from LakeFS."""
    logger.info(f"Getting future tag for {repo}")
    return await resolve_future_tag(repo)

@activity.defn
async def get_qlever_storage_size(repo: str, branch: str) -> str:
    """
    Query LakeFS for the size of the QLever directory, add a 10% buffer,
    and return a PVC storage size string (e.g., '15Gi').
    Minimum size is set to 5Gi.
    """
    logger.info(f"Calculating QLever storage size for {repo}@{branch}")
    # Size of the qlever directory in bytes
    size_bytes = await get_lakefs_prefix_size(repo, branch, "qlever/")
    
    # Add 10% buffer
    buffered_size_bytes = size_bytes * 1.10
    
    # Convert to GiB
    size_gib = buffered_size_bytes / (1024**3)
    
    # Ensure minimum 2Gi and round up to next integer
    final_size_gib = max(2, int(size_gib) + 1)

    result = f"{final_size_gib}Gi"
    logger.info(f"Calculated PVC storage size: {result}")
    return result


# ---------------------------------------------------------------------------
# LDF aggregator activities
# ---------------------------------------------------------------------------

def _ldf_sync_image() -> str:
    """Image used by the per-KG sync Job. Falls back to the kace pod image
    if LDF_SYNC_IMAGE is not configured. Reads kace-temporal-worker pod's
    own image at runtime to stay aligned with the deployed kace version."""
    if app_config.ldf_sync_image:
        return app_config.ldf_sync_image
    # Try to read this pod's own image so the sync Job runs the same kace code
    try:
        from kubernetes import client as k8s_client
        pod_name = os.environ.get("HOSTNAME")
        if pod_name:
            api = k8s_client.CoreV1Api()
            pod = api.read_namespaced_pod(name=pod_name, namespace=app_config.k8s_namespace)
            return pod.spec.containers[0].image
    except Exception as e:
        logger.warning(f"Could not auto-detect kace image, using fallback: {e}")
    return "containers.renci.org/frink/kace:latest"


def _ldf_sync_env() -> list[dict]:
    """Env vars passed to the sync Job — needs lakefs creds + URL."""
    return [
        {"name": "LAKEFS_URL", "value": app_config.lakefs_url},
        {"name": "LAKEFS_ACCESS_KEY", "value": app_config.lakefs_access_key},
        {"name": "LAKEFS_SECRET_KEY", "value": app_config.lakefs_secret_key},
        {"name": "PYTHONPATH", "value": "/apps/src"},
        {"name": "LOCAL_DATA_DIR", "value": "/tmp"},
        {"name": "K8S_NAMESPACE", "value": app_config.k8s_namespace},
    ]


@activity.defn
async def fetch_all_kgs() -> list[dict]:
    """Returns list of {repo, shortname, hdt_path} for every registry KG with
    a lakefs repo."""
    kg_config = await KGConfig.from_git()
    out = []
    for kg in kg_config.kgs:
        if not kg.frink_options or not kg.frink_options.lakefs_repo or not kg.shortname:
            continue
        out.append({
            "repo": kg.frink_options.lakefs_repo,
            "shortname": kg.shortname,
            "hdt_path": "hdt",
        })
    return out


@activity.defn
async def get_lakefs_latest_commit_activity(repo: str, ref: str = "main") -> str:
    return await get_latest_commit(repo, ref)


@activity.defn
async def resolve_kg_ref(repo: str) -> dict:
    """Pick the ref to sync from. Prefer the highest existing tag for a stable
    snapshot; fall back to main HEAD if no tags. Always also returns the
    underlying commit ID for change-detection."""
    tag = await get_latest_tag(repo)
    ref = tag if tag else "main"
    commit = await get_latest_commit(repo, ref)
    return {"ref": ref, "tag": tag, "commit": commit}


@activity.defn
async def resolve_qlever_refs(only_kg: list = None) -> dict:
    """Resolve the ref+commit for every lakefs repo that feeds the federated
    qlever index. Each KG uses its latest semver tag (falling back to 'main'
    if no tags), except wikidata which always tracks 'main' (override). The
    s2-builds repo is always pinned to its latest semver tag.

    For every KG, an `objects/stat` pre-flight is performed against the
    chosen ref + remote_path. KGs whose source object is missing are dropped
    from `kg_refs` and accumulated in `skipped` so the workflow can Slack-
    report them without failing the build.

    Returns:
      {
        "kg_refs":  {repo_id: {"shortname", "ref", "commit", "remote_path"}},
        "skipped":  [{"repo", "shortname", "ref", "remote_path", "reason"}],
        "s2_repo":  str,
        "s2_tag":   str,
        "s2_commit": str,
      }
    """
    kg_config = await KGConfig.from_git()
    skip_repos = {'semopenalex'}
    overrides = PER_REPO_LAKEFS_OVERRIDES

    kg_refs: dict = {}
    skipped: list = []
    for kg in kg_config.kgs:
        if not (kg.frink_options and kg.frink_options.lakefs_repo and kg.shortname):
            continue
        if kg.shortname in skip_repos:
            continue
        if only_kg and kg.shortname not in only_kg:
            continue
        repo = kg.frink_options.lakefs_repo
        # Override lookup tries shortname first, then lakefs_repo. Lets the
        # dict be keyed however a future contributor finds clearest.
        override = overrides.get(kg.shortname) or overrides.get(repo) or {}
        if "ref" in override:
            ref = override["ref"]
        else:
            tag = await get_latest_tag(repo)
            ref = tag if tag else "main"
        remote_path = override.get("remote_path", "nt/graph.nt.gz")

        # Pre-flight: skip (don't fail) if the source file is missing.
        try:
            exists = await object_exists(repo, ref, remote_path)
        except Exception as e:
            exists = False
            reason = f"stat failed: {e}"
        else:
            reason = "object not found"

        if not exists:
            logger.warning(f"Skipping {kg.shortname} ({repo}@{ref}:{remote_path}) — {reason}")
            skipped.append({
                "repo":        repo,
                "shortname":   kg.shortname,
                "ref":         ref,
                "remote_path": remote_path,
                "reason":      reason,
            })
            continue

        commit = await get_latest_commit(repo, ref)
        kg_refs[repo] = {
            "shortname":   kg.shortname,
            "ref":         ref,
            "commit":      commit,
            "remote_path": remote_path,
        }

    s2_tag = await get_latest_tag(S2_LAKEFS_REPO)
    if not s2_tag:
        raise Exception(f"No tags on '{S2_LAKEFS_REPO}' repo; cannot pin s2 sources")
    s2_commit = await get_latest_commit(S2_LAKEFS_REPO, s2_tag)
    return {
        "kg_refs":   kg_refs,
        "skipped":   skipped,
        "s2_repo":   S2_LAKEFS_REPO,
        "s2_tag":    s2_tag,
        "s2_commit": s2_commit,
    }


@activity.defn
async def read_qlever_state() -> dict:
    from k8s import qlever_state
    return qlever_state.read_state()


@activity.defn
async def write_qlever_state(state: dict) -> None:
    from k8s import qlever_state
    qlever_state.write_state(state)


@activity.defn
async def create_qlever_index_pvc(build_id: str, image: str) -> str:
    from k8s import qlever_pvc
    return qlever_pvc.create_index_pvc(build_id, image)


@activity.defn
async def gc_qlever_index_pvcs(state: dict, now_iso: str) -> dict:
    from k8s import qlever_pvc
    return qlever_pvc.gc_index_pvcs(state, now_iso)


@activity.defn
async def read_ldf_state() -> dict:
    return ldf_server_manager.read_state_configmap()


@activity.defn
async def write_ldf_state(commits: dict) -> None:
    ldf_server_manager.apply_state_configmap(commits)


@activity.defn
async def ensure_ldf_pvc() -> None:
    ldf_server_manager.apply_pvc()


@activity.defn
async def submit_ldf_sync_job(repo: str, ref: str, shortname: str, hdt_path: str = "hdt") -> str:
    image = _ldf_sync_image()
    env = _ldf_sync_env()
    return ldf_server_manager.submit_sync_job(
        repo=repo, ref=ref, shortname=shortname,
        image=image, env_pairs=env, hdt_path=hdt_path,
    )


@activity.defn
async def wait_ldf_sync_job(job_name: str, timeout_seconds: int = 7200) -> bool:
    # Run the blocking watch in a thread to avoid blocking the asyncio loop.
    return await asyncio.to_thread(
        ldf_server_manager.wait_for_job, job_name, timeout_seconds
    )


@activity.defn
async def apply_ldf_config_and_rollout() -> str:
    """Renders the LDF config-map (datasource list) from current registry +
    extras, applies it, and patches the deployment annotation to trigger a
    rolling restart. Returns the new config hash."""
    kg_config = await KGConfig.from_git()
    datasources = ldf_server_manager.build_datasources(kg_config)
    config_hash = ldf_server_manager.compute_config_hash(datasources)
    # Apply config-map
    ldf_server_manager.create_or_update_configmap_k8s({
        "kg_name": "all",
        "host_name": app_config.ldf_host_name,
        "kg_config": kg_config,
    })
    # Apply / create deployment with current hash
    ldf_server_manager.create_or_update_deployment({
        "config_hash": config_hash,
        "host_name": app_config.ldf_host_name,
    })
    # Apply service if not present
    try:
        ldf_server_manager.create_or_update_service({})
    except Exception as e:
        logger.warning(f"LDF service apply skipped: {e}")
    # Apply route (gateway HTTPRoute + HealthCheckPolicy, or Ingress)
    route_params = {"host_name": app_config.ldf_host_name}
    if app_config.networking_mode == "gateway":
        ldf_server_manager.create_or_update_httproute(route_params)
        ldf_server_manager.create_or_update_healthcheck(route_params)
    else:
        ldf_server_manager.create_or_update_ingress(route_params)
    # Force a rolling restart by re-patching annotation (idempotent)
    ldf_server_manager.rolling_restart(config_hash)
    return config_hash
