from celery import Celery
from celery.schedules import crontab
from k8s.podman import JobMan
from k8s import fuseki_server_manager, ldf_server_manager
from models.lakefs_models import LakefsMergeActionModel, LakefTagCreationModel
from models.kg_metadata import KGConfig, KG
from lakefs_util.io_util import resolve_commit, download_file_from_latest_tag
from log_util import LoggingUtil
from config import config
import os
import requests
from canary.slack import slack_canary
from canary.mail import mail_canary
import asyncio

logger = LoggingUtil.init_logging(__name__)


app = Celery('celery_tasks',
             broker='redis://localhost:6379',
             backend='rpc://',
             include=['celery_tasks'])


app.conf.update(
    result_expires=3600,
    broker_connection_retry_on_startup=True,
    # beat_schedule={
    #     'weekend-maintenance': {
    #         'task': 'celery_tasks.celery.qlever_index',
    #         'schedule': crontab(hour=0, minute=0, day_of_week='sat,sun'),
    #     },
    # }
)


# Optional configuration, see the application user guide.
@app.task(ignore_result=True)
@slack_canary.slack_notify_on_failure("⚠️ To HDT conversion fail")
def create_hdt_conversion_job(action_payload,
                              files_list,
                              doc_path,
                              kg_title,
                              cpu=3,
                              memory="28Gi",
                              ephemeral="2Gi",
                              java_opts="-Xmx25G -Xms25G -Xss512m -XX:+UseParallelGC",
                              mem_size="25G",
                              convert_to_hdt=True,
                              hdt_path="/"
                              ):
    logger.info(f"Convert to HDT ***************************** {type(convert_to_hdt)} : {convert_to_hdt}")
    lakefs_payload = LakefsMergeActionModel(**action_payload)
    job = JobMan()
    logger.info(lakefs_payload)
    job_name = f'{lakefs_payload.hook_id}-{lakefs_payload.repository_id[:10]}-{lakefs_payload.branch_id.replace("_","-")[:10]}-{lakefs_payload.commit_id[:10]}'
    logger.info(f"Running as user: {os.getuid()}")

    working_dir = str(os.path.join("/mnt/repo", lakefs_payload.repository_id, lakefs_payload.branch_id))
    logger.info("working directory: " + working_dir)
    kg_config = asyncio.run(KGConfig.from_git()).get_by_repo(lakefs_payload.repository_id)
    if convert_to_hdt:
        logger.info(f'Job name: {job_name}')
        # report the job id back ...
        job.run_job(job_type="hdt-job",
                    job_name=job_name,
                    repo=lakefs_payload.repository_id,
                    branch=lakefs_payload.branch_id,
                    args=files_list,
                    resources={
                        "requests": {
                            "cpu": cpu,
                            "memory": memory,
                            "ephemeral-storage": ephemeral
                        },
                        "limits": {
                            "cpu": cpu,
                            "memory": memory,
                            "ephemeral-storage": ephemeral
                        }
                    },
                    env_vars={
                        "GH_TOKEN": config.gh_token,
                        "GH_HANDLES": ",".join(kg_config.github_handles),
                        "JAVA_OPTIONS": java_opts,
                        "MEM_SIZE": mem_size,
                        "WORKING_DIR": working_dir,
                        "REPO_NAME": lakefs_payload.repository_id,
                        "COMMIT_ID": lakefs_payload.source_ref,
                        "KG_NAME": kg_title,
                    })
        job.watch_job(job_name=job_name)
        logger.info(f'Job {job_name} finished.')

    # lets create documentation job
    if convert_to_hdt:
        hdt_path = working_dir + '/hdt'
    else:
        hdt_path = working_dir + '/' + hdt_path

    doc_job_name = job_name + '-doc'
    job.run_job(
        job_name=doc_job_name,
        job_type='documentation-job',
        env_vars={
            "GH_TOKEN": config.gh_token,
            "WORKING_DIR": working_dir
        },
        args=[
            doc_path,
            hdt_path,
            kg_title,
            kg_title + "-documentation-update"
        ],
        repo=lakefs_payload.repository_id,
        branch=lakefs_payload.branch_id,
        resources={
            "requests": {
                "cpu": cpu,
                "memory": memory,
                "ephemeral-storage": ephemeral
            },
            "limits": {
                "cpu": cpu,
                "memory": memory,
                "ephemeral-storage": ephemeral
            }
        }
    )
    # Disable job watch
    # job.watch_job(job_name=doc_job_name)

    requests.post(config.hdt_upload_callback_url, params={
        "converted_hdt": convert_to_hdt
    }, json=action_payload)



@app.task(ignore_result=True)
@slack_canary.slack_notify_on_failure("⚠️ Neo4j to RDF fail")
def create_neo4j_conversion_job(action_payload,
                                dump_files_list,
                                json_dump_files_list,
                                rdf_mapping_config):
    """
    Converts Neo4j dump files to ttl based on a mapping config. The mapping config is expected to be defined
    in the KG registry file. It works by converting dump files to json exports then to rdf (.nt) formatted file.
    If json exports are available it skips the first dump to json job.
    :param action_payload: Lakefs action payload
    :param dump_files_list: List of files with .dump
    :param json_dump_files_list:  list of neo4j json exports.
    :param rdf_mapping_config:
    :return:
    """
    lakefs_payload = LakefsMergeActionModel(**action_payload)
    job = JobMan()
    logger.info(lakefs_payload)
    json_job_name = f'{lakefs_payload.hook_id}-{lakefs_payload.repository_id[:10]}-{lakefs_payload.branch_id}-{lakefs_payload.commit_id[:10]}-to-json'
    rdf_job_name = f'{lakefs_payload.hook_id}-{lakefs_payload.repository_id[:10]}-{lakefs_payload.branch_id}-{lakefs_payload.commit_id[:10]}-to-rdf'
    working_dir = os.path.join("/mnt/repo", lakefs_payload.repository_id, lakefs_payload.branch_id)
    logger.info(f'Job name: {json_job_name}')
    json_dump_files_list = json_dump_files_list or []

    if not len(json_dump_files_list):
        # Run dump to json conversion iff the repository contains just dump files.
        logger.info("creating a neo4j to json conversion job...")
        job.run_job(job_type="neo4j-json-job",
                    job_name=json_job_name,
                    repo=lakefs_payload.repository_id,
                    branch=lakefs_payload.branch_id,
                    args=dump_files_list,
                    resources={
                        "limits": {
                            "cpu": 3,
                            "memory": "20Gi"
                        }
                    },
                    env_vars={
                        "JAVA_OPTIONS": "-Xmx20G -XX:+UseParallelGC",
                        "WORKING_DIR": working_dir
                    })
        job.watch_job(job_name=json_job_name)
        logger.info(f'Job {rdf_job_name} finished.')
        source_files = "neo4j-export/neo4j-apoc-export.json"
    else:
        source_files = json_dump_files_list[0]
    neo4j_export_json_location = working_dir.rstrip('/') + '/' + source_files.lstrip('/')

    job.run_job(job_type="neo4j-rdf-job",
                job_name=rdf_job_name,
                repo=lakefs_payload.repository_id,
                branch=lakefs_payload.branch_id,
                args=[
                    "-i",
                    neo4j_export_json_location,
                    "-c",
                    rdf_mapping_config,
                    "-w",
                    working_dir
                ],
                resources={
                    "limits": {
                        "cpu": 3,
                        "memory": "20Gi"
                    }
                },
                env_vars={
                    "JAVA_OPTIONS": "-Xmx20G -XX:+UseParallelGC",
                    "WORKING_DIR": os.path.join("/mnt/repo", lakefs_payload.repository_id, lakefs_payload.branch_id)
                })
    job.watch_job(job_name=rdf_job_name)
    logger.info(f'Job {rdf_job_name} finished.')
    # @todo send source files back to the callback to let it ignore them .
    requests.post(config.neo4j_upload_callback_url, json=action_payload)


@app.task(ignore_result=True)
@slack_canary.slack_notify_on_failure("⚠️ Deployment Error")
def create_deployment(kg_config: dict, cpu: str, memory: str, lakefs_action):
    lakefs_action: LakefTagCreationModel = LakefTagCreationModel(**lakefs_action)
    kg_config: KG = KG(**kg_config)
    # this will be given to the pod. So if version changes pod is restarted :)
    kg_name = kg_config.shortname
    annotations = {
        "kg-name": kg_name,
        "version": lakefs_action.tag_id,
        "lakefs-repository": lakefs_action.repository_id,
        "commit": lakefs_action.commit_id
    }
    resources = {
        "requests": {
            "memory": memory,
        },
        "limits": {
            "memory": memory,
        }
    }
    if cpu:
        resources["limits"]["cpu"] = cpu
    # Create fuseki server
    fuseki_server_manager.create_all(
        parameters={
            "kg_name": kg_name
        },
        annotations=annotations,
        resources=resources
    )
    retries = 10
    server_up = fuseki_server_manager.wait_for_services_to_be_running(parameters={
        "kg_name": kg_name
    }, annotations=annotations, max_retries=retries)
    if not server_up:
        raise Exception(f"Fuseki deployment for {kg_name} - version: {lakefs_action.tag_id} failed. Deployment "
                        f"Artifacts didn't appear to be running after {retries} retries.")

    logger.info(f'{kg_name} deployment created.')
    # @TODO annotations might need to be changed
    ldf_server_manager.create_all(
        parameters={
            "kg_name": kg_name,
            "host_name": config.frink_address,
        },
        annotations=annotations,
        # Use default resources in the template
        resources=None
    )
    logger.info(f'updated ldf server deployment')
    logger.info(f'updated federation server deployment')
    job_name = f'spider-cl-{kg_name}-{lakefs_action.commit_id[:10]}'
    try:
        job = JobMan()
        logger.info(f"creating spider client job")
        commit_data = resolve_commit(lakefs_action.repository_id, lakefs_action.source_ref)
        logger.info(f'Job name: {job_name}')
        # report the job id back ...
        job.run_job(job_type="spider-job",
                    job_name=job_name,
                    repo=lakefs_action.repository_id,
                    branch=lakefs_action.commit_id,
                    args=["/app/spider-client.py",
                          "--commit-id",
                          lakefs_action.commit_id,
                          "--graph-name",
                          kg_name,
                          "--committer-name",
                          commit_data.committer,
                          "--ip",
                          config.spider_ip,
                          "--port",
                          str(config.spider_port)
                          ]
                    )
        job.watch_job(job_name=job_name)
        logger.info(f'Job {job_name} finished.')
    except Exception as e:
        logger.exception(f'Spider Job failed {job_name} : {str(e)}')
    kg_config = asyncio.run(KGConfig.from_git()).get_by_repo(lakefs_action.repository_id)
    if kg_config.emails:
        mail_canary.send_deployed_email(
            kg_name=kg_config.title,
            version=lakefs_action.tag_id,
            recipient_email=",".join(kg_config.emails),
        )

@app.task
def qlever_index():
    logger.info("Starting QLever Index maintenance task")
    job = JobMan()
    job_name = "qlever-index-maintenance"
    kg_config: KGConfig = KGConfig.from_git_sync()
    working_dir = "/shared/nt-conversions"

    async def download_all(kgs):
        tasks = []
        for kg in kgs:
            if kg.frink_options and kg.frink_options.lakefs_repo:
                repo = kg.frink_options.lakefs_repo
                local_path = os.path.join(working_dir, repo, "nt", "graph.nt.gz")
                tasks.append(download_file_from_latest_tag(repo, "nt/graph.nt.gz", local_path))
        if tasks:
            await asyncio.gather(*tasks)

    asyncio.run(download_all(kg_config.kgs))
    skip_repos = [
        'geoconnex',
        'spatial-kg',
        'hydrology-kg',
        'dream-kg',
        'scales-kg',
        'ubergraph',
        'wikidata',
        'sem-open-alex-kg'
    ]
    build_args = [
        '-c',
        'ulimit -Sn 1048576; IndexBuilderMain -i frink -s ../frink-qlever.settings.json'

    ]
    for kg in kg_config.kgs:
        if kg in skip_repos:
            continue
        build_args.append(
            f'-f <(gunzip -c {working_dir}/{kg.shortname}/nt/graph.nt.gz)',
            f'-g https://purl.org/okn/frink/kg/{kg.shortname}',
            '-F nt'
        )

    additional_kg = {
        # s2 data
        "geoconnex#s2": {
            "file_path": f'{working_dir}/geoconnex/geoconnex-geo.nt.gz'
        },
        "hydrologykg#s2": {
            "file_path": f'{working_dir}/hydrology-geo.nt.gz'
        },
        "sockg#s2": {
            "file_path": f'{working_dir}/sockg-geo.nt.gz'
        },
        'spatialkg#s2': {
            "file_path": f'{working_dir}/spatialkg-geo.nt.gz'
        },
        "ufokn#s2": {
            "file_path": f'{working_dir}/ufokn-geo.nt.gz'
        },
        # big graph
        "ubergraph": {
            "file_path": f'{working_dir}/ubergraph.nt.gz'
        },
        "wikidata": {
            "file_path": f'{working_dir}/wikidata.nt.gz'
        },
        # kgs with doc issues
        "dream-kg": {
            "file_path": f'{working_dir}/dream-kg.nt.gz'
        },
        "spatial-kg": {
            "file_path": f'{working_dir}/spatial-kg.nt.gz'
        },
        "scales-kg": {
            "file_path": f'{working_dir}/scales-kg.nt.gz | grep -v "<http://stko-kwg.geog.ucsb.edu/lod/ontology/cellID>"'
        },
        "geoconnex": {
            "file_path": f'{working_dir}/geoconnex.nt.gz'
        },
        "hydrology-kg": {
            "file_path": f'{working_dir}/hydrology-kg.nt.gz'
        },
    }
    build_args += [
        f'-f <(gunzip -c {value["file_path"]})' + f'-g https://purl.org/okn/frink/kg/{key}' + '-F nt' for key, value in additional_kg.items()
    ]


    
    job.run_job(
        job_type="qlever-index-job",
        job_name=job_name,
        repo="",
        branch="",
        args=[],
        resources={
            "requests": {
                "cpu": "1",
                "memory": "4Gi"
            },
            "limits": {
                "cpu": "8",
                "memory": "100Gi"
            }
        },
        env_vars={
            "JAVA_OPTIONS": "-Xmx24G"
        }
    )

    job.watch_job(job_name=job_name)
    logger.info(f"Job {job_name} finished.")


if __name__ == '__main__':
    app.start()