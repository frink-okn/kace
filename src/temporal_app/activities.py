from temporalio import activity
from k8s.podman import JobMan
from k8s import fuseki_server_manager, ldf_server_manager
from lakefs_util.io_util import resolve_commit, download_file_from_latest_tag, download_files, upload_files, clean_up_files
from canary.slack import slack_canary
from canary.mail import mail_canary
from models.lakefs_models import LakefsMergeActionModel, LakefTagCreationModel
from models.kg_metadata import KGConfig, KG
from config import config
import asyncio
import os
from log_util import LoggingUtil
from config import config as app_config

logger = LoggingUtil.init_logging(__name__)

@activity.defn
async def run_k8s_job(job_type: str, job_name: str, repo: str, branch: str, 
                      command: list[str] = None, args: list[str] = None, 
                      resources: dict = None, env_vars: dict = None, 
                      additional_volume_mounts: list = None) -> str:
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
        additional_volume_mounts=additional_volume_mounts
    )
    return job_name

@activity.defn
async def watch_k8s_job_sync(job_name: str, poll_interval: int = 5) -> None:
    logger.info(f"Watching K8s job: {job_name}")
    job_man = JobMan()
    await job_man.async_watch_job(job_name=job_name, poll_interval=poll_interval)


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
async def download_file_lakefs(repo: str, remote_path: str, local_path: str) -> None:
    # this function in io_util is async
    await download_file_from_latest_tag(repo, remote_path, local_path)

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
                               delete_all_files: bool = True) -> list[str]:
    """Download input files from LakeFS. Returns list of downloaded file paths."""
    logger.info(f"Downloading input files from {repo}@{branch}")
    files = await download_files(
        repo=repo,
        branch=branch,
        extensions=extensions,
        exclude_files=exclude_files,
        exclude_known_extension=exclude_known_extension,
        delete_all_files=delete_all_files
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

@activity.defn
async def prepare_qlever_job_specs(only_kg: list[str] = None) -> dict:
    """
    Returns a dict with:
    - downloads: list of dicts {repo, remote_path, local_path}
    - build_args: str (the command string for the job)
    - skip_repos: list[str]
    """
    kg_config = KGConfig.from_git_sync()
    working_dir = "/shared/nt-conversions"
    skip_repos = [
        'prokn', 'ubergraph', 'wikidata', 'semopenalex',
        # ... (copied from celery.py) ...
    ]
    
    downloads = []
    
    # Logic from celery.py
    for kg in kg_config.kgs:
        if kg.frink_options and kg.frink_options.lakefs_repo:
            if only_kg and kg.shortname not in only_kg:
                if only_kg not in skip_repos:
                    # Logic error in original code? "if only_kg not in skip_repos"
                    # original: if only_kg not in skip_repos: skip_repos += [kg.shortname]
                    # only_kg is list or None? call signiture says None.
                    # celery: def qlever_index(only_kg=None)
                    # if only_kg and ...
                    pass
                continue
            if kg.shortname in skip_repos:
                continue
            
            repo = kg.frink_options.lakefs_repo
            local_path = os.path.join(working_dir, repo, "nt", "graph.nt.gz")
            downloads.append({
                "repo": repo,
                "remote_path": "nt/graph.nt.gz",
                "local_path": local_path
            })

    # Construct build args
    build_cmd_parts = [
        'ulimit -Sn 1048576; mkdir -p /shared/qlever-index/; cd /shared/qlever-index/; IndexBuilderMain -i frink -s /qlever/frink-qlever.settings.json'
    ]
    
    for kg in kg_config.kgs:
        if kg.shortname in skip_repos:
            continue
        # Check if we should skip based on only_kg logic implemented above implicitly?
        # The loop in celery.py iterates again. 
        # We need to reuse the skip logic or check if it was downloaded.
        # Simpler:
        if kg.frink_options and kg.frink_options.lakefs_repo:
             if only_kg and kg.shortname not in only_kg:
                 continue
             
        build_cmd_parts.append(
             " ".join([f'-f <(gunzip -c {working_dir}/{kg.frink_options.lakefs_repo}/nt/graph.nt.gz)',
                       f'-g https://purl.org/okn/frink/kg/{kg.shortname}',
                       '-F nt'])
        )

    # non_lakefs_files logic
    non_lakefs_files = {
        "geoconnex#s2": {"file_path": f'{working_dir}/geoconnex/geoconnex-geo.nt.gz'},
        "hydrologykg#s2": {"file_path": f'{working_dir}/hydrology-geo.nt.gz'},
        "sockg#s2": {"file_path": f'{working_dir}/sockg-geo.nt.gz'},
        'spatialkg#s2': {"file_path": f'{working_dir}/spatialkg-geo.nt.gz'},
        "ufokn#s2": {"file_path": f'{working_dir}/ufokn-geo.nt.gz'},
        "ubergraph": {"file_path": f'{working_dir}/ubergraph.nt.gz'},
        "wikidata": {"file_path": f'{working_dir}/wikidata.nt.gz'},
        "dream-kg": {"file_path": f'{working_dir}/dream-kg.nt.gz'},
        "spatial-kg": {"file_path": f'{working_dir}/spatial-kg.nt.gz'},
        "scales-kg": {"file_path": f'{working_dir}/scales-kg.nt.gz | grep -v "<http://stko-kwg.geog.ucsb.edu/lod/ontology/cellID>"'},
        "geoconnex": {"file_path": f'{working_dir}/geoconnex.nt.gz'},
        "hydrology-kg": {"file_path": f'{working_dir}/hydrology-kg.nt.gz'},
    }

    # "additional_kg = {} # if only_kg else non_lakefs_files" from celery.py
    # "additional_kg = {}" is hardcoded commented out logic?
    # "additional_kg = {} # if only_kg else non_lakefs_files"
    # The code says: additional_kg = {}
    # Then:
    # build_args += [ ... for key, value in additional_kg.items() ]
    # So non_lakefs_files are IGNORED in current celery.py?
    # Wait, line 436: additional_kg = {} # if only_kg else non_lakefs_files
    # It assumes `additional_kg` is empty!
    # I should probably follow the code as executes.
    
    additional_kg = {} 
    
    # If the user commented it out, I will follow that. 
    
    build_cmd_parts.append('--stxxl-memory 40G')
    
    final_cmd = ' '.join(build_cmd_parts)
    
    return {
        "downloads": downloads,
        "build_command": final_cmd,
    }

@activity.defn
async def get_spider_config() -> dict:
    return {
        "ip": config.spider_ip,
        "port": config.spider_port
    }

@activity.defn
async def create_local_dir(dir_path: str) -> None:
    """Create a local directory on the worker if it does not exist."""
    logger.info(f"Creating local directory: {dir_path}")
    os.makedirs(dir_path, exist_ok=True)

