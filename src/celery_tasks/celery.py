from celery import Celery
from k8s.podman import JobMan
from k8s.server_man import fuseki_server_manager, federation_server_manager, ldf_server_manager
from models.lakefs_models import LakefsMergeActionModel, LakefTagCreationModel
from models.kg_metadata import KGConfig, KG
from lakefs_util.io_util import resolve_commit
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
    job.watch_job(job_name=doc_job_name)

    requests.post(config.hdt_upload_callback_url, params={
        "converted_hdt": convert_to_hdt
    }, json=action_payload)


app.conf.update(
    result_expires=3600,
    broker_connection_retry_on_startup=True,
)


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
    # Create federation server
    # @TODO annotations might need to change
    # federation_server_manager.create_all(parameters={},
    #                                      annotations=annotations,
    #                                      # Use default resources in the template
    #                                      resources=None
    #                                      )
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
    mail_canary.send_deployed_email(
        kg_name=kg_config.title,
        version=lakefs_action.tag_id,
        recipient_email=",".join(kg_config.contact.email),
    )

if __name__ == '__main__':
    app.start()