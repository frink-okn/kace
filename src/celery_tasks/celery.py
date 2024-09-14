from celery import Celery
from k8s.podman import JobMan
from k8s.fuseki_man import create_k8s_objects
from models.lakefs_models import LakefsMergeActionModel, LakefTagCreationModel
from lakefs_util.io_util import resolve_commit
from log_util import LoggingUtil
from config import config
import requests


logger = LoggingUtil.init_logging(__name__)


app = Celery('celery_tasks',
             broker='redis://localhost:6379',
             backend='rpc://',
             include=['celery_tasks'])

# Optional configuration, see the application user guide.
app.conf.update(
    result_expires=3600,
    broker_connection_retry_on_startup=True,
)


@app.task(ignore_result=True)
def create_hdt_conversion_job(action_payload, files_list):
    lakefs_payload = LakefsMergeActionModel(**action_payload)
    job = JobMan()
    logger.info(lakefs_payload)
    job_name = f'{lakefs_payload.hook_id}-{lakefs_payload.repository_id}-{lakefs_payload.branch_id}-{lakefs_payload.commit_id[:10]}'

    logger.info(f'Job name: {job_name}')
    # report the job id back ...
    job.run_job(job_type="hdt-job",
                job_name=job_name,
                repo=lakefs_payload.repository_id,
                branch=lakefs_payload.branch_id,
                args=files_list,
                resources={
                    "limits": {
                        "cpu": 3,
                        "memory": "20Gi"
                    }
                },
                env_vars={
                    "JAVA_OPTIONS": "-Xmx20G -XX:+UseParallelGC"
                })
    job.watch_pod(job_name=job_name)
    logger.info(f'Job {job_name} finished.')
    requests.post(config.hdt_upload_callback_url, json=action_payload)

@app.task(ignore_result=True)
def create_deployment(kg_name: str, cpu: int, memory: str, lakefs_action):
    lakefs_action : LakefTagCreationModel = LakefTagCreationModel(**lakefs_action)
    # this will be given to the pod. So if version changes pod is restarted :)
    annotations = {
        "kg-name": kg_name,
        "version": lakefs_action.tag_id,
        "lakefs-repository": lakefs_action.repository_id,
        "commit": lakefs_action.commit_id
    }
    create_k8s_objects(kg_name=kg_name,
                       namespace=config.k8s_namespace,
                       cpu=cpu,
                       memory=memory,
                       pvc_name=config.shared_pvc_name,
                       annotations=annotations)
    # @TODO probably worth doing some tests here, something like liveliness check

    logger.info(f'{kg_name} deployment created.')

    job = JobMan()
    logger.info(f"creating spider client job")

    job_name = f'spider-cl-{kg_name}-{lakefs_action.commit_id[:10]}'
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
    job.watch_pod(job_name=job_name)
    logger.info(f'Job {job_name} finished.')
    # requests.post(config.hdt_upload_callback_url, json=action_payload)

if __name__ == '__main__':
    app.start()