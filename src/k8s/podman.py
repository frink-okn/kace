import time

import kubernetes.client.exceptions
from kubernetes import client, config, watch
import yaml
import os
from typing import Dict, List
from log_util import LoggingUtil
from config import config as app_conf
logger = LoggingUtil.init_logging(__name__)

mapping = {
    "hdt-job": os.path.dirname(os.path.realpath(__file__)) + os.path.join(os.path.sep + "templates", "hdt-conversion.yaml"),
    "spider-job": os.path.dirname(os.path.realpath(__file__)) + os.path.join(os.path.sep + "templates", "spider-client.yaml"),
    "noe4j-rdf-job": os.path.dirname(os.path.realpath(__file__)) + os.path.join(os.path.sep + "templates", "neo4j-rdf-job.yaml"),
    "documentation-job": os.path.dirname(os.path.realpath(__file__)) + os.path.join(os.path.sep + "templates", "documentation-job.yaml")

    ## add other pods here
}

config.load_incluster_config()
# config.load_kube_config(config_file='/mnt/c/Users/kebedey/kubeconfig/kubeconfig-sterling-kebedey-kebedey', context='kebedey')
class JobMan:
    def __init__(self):
        self.job_configs = {}
        # @TODO make this a config
        self.namespace = app_conf.k8s_namespace
        for job_type, pod_dir in mapping.items():
            with open(pod_dir) as stream:
                self.job_configs[job_type] = yaml.safe_load(stream)
        self.job_objects = self.init_job_objects()
        logger.info(f"Initialized K8s Job manager: {self.job_configs} ")

    def init_job_objects(self) -> Dict[str, client.V1Job]:
        job_objects = {}
        for job_type in self.job_configs:
            job_objects[job_type] = self.init_job_object(name=job_type, **self.job_configs[job_type])
        return job_objects

    @staticmethod
    def init_job_object(name: str, image: str, command: List[str]) -> client.V1Job:
        # create the pod
        return client.V1Job(
            api_version="batch/v1",
            kind="Job",
            metadata=client.V1ObjectMeta(name=name),
            spec=client.V1JobSpec(
                template=client.V1PodTemplateSpec(
                    spec=client.V1PodSpec(
                        containers=[
                            client.V1Container(
                                name=name,
                                image=image,
                                command=command,
                                tty=True,
                                stdin=True,
                            )
                        ],
                        restart_policy="Never",
                        volumes=[
                            client.V1Volume(
                                name="data",
                                persistent_volume_claim=client.V1PersistentVolumeClaimVolumeSource(
                                    claim_name=app_conf.local_pvc_name
                                )
                            )
                        ]
                    )
                ),
                backoff_limit=4,
            ),
        )

    def run_job(self, job_type, job_name, repo: str, branch: str, command: List[str] = None, args: List[str] = None,
                resources=None, env_vars=None):
        if env_vars is None:
            env_vars = dict()
        job: kubernetes.client.V1Job = self.job_objects[job_type]
        # override default name, by default its named as the job-type
        job.metadata.name = job_name
        # override command and args
        logger.info("job man recieved job {} - {}".format(job_type, job_name))
        pod_template: kubernetes.client.V1PodSpec = job.spec.template.spec

        if command:
            pod_template.containers[0].command = command
        if args:
            logger.info("pod args {}".format(args))
            pod_template.containers[0].args = args
        if resources:
            resources = client.V1ResourceRequirements(**resources)
            pod_template.containers[0].resources = resources
        if env_vars:
            env = [kubernetes.client.V1EnvVar(name=key, value=value) for key, value in env_vars.items()]
            pod_template.containers[0].env = env
        # @TODO not all job containers need this but ok ...
        volume_mounts = [
            client.V1VolumeMount(
                name="data",
                mount_path="/mnt/repo",
                # sub_path=f"{repo}/{branch}"
            )
        ]
        pod_template.containers[0].volume_mounts = volume_mounts

        api = client.BatchV1Api()
        logger.info(f"removing previous jobs {job_name} ")
        self.remove_job(job_name)
        return api.create_namespaced_job(namespace=self.namespace, body=job)

    def watch_job(self, job_name, poll_interval=5):
        """
        Watch the Job's status until it completes (succeeds or fails).
        This function will continue polling indefinitely until the Job reaches a terminal state.

        Args:
            job_name (str): The name of the Job.
            poll_interval (int): Seconds to wait between successive status checks.

        Raises:
            Exception: If the Job fails.
        """
        batch_v1 = client.BatchV1Api()

        while True:
            try:
                job = batch_v1.read_namespaced_job(name=job_name, namespace=self.namespace)
            except client.exceptions.ApiException as e:
                logger.error(f"Failed to fetch Job '{job_name}': {e}")
                raise Exception(f"Failed to fetch Job '{job_name}': {e} , likely the job was never created.")

            # Use 0 as default if the values are None.
            succeeded = job.status.succeeded or 0
            failed = job.status.failed or 0
            # If backoffLimit is not set, Kubernetes defaults to 6 retries.
            backoff_limit = job.spec.backoff_limit if job.spec.backoff_limit is not None else 6

            logger.info(f"Job '{job_name}' status: succeeded={succeeded}, failed={failed}")

            if succeeded > 0:
                logger.info(f"Job '{job_name}' completed successfully.")
                return

            # Consider the Job failed if the number of failures reaches the backoff limit.
            if failed >= backoff_limit:
                logger.error(f"Job '{job_name}' failed after {failed} attempts.")
                raise Exception(f"Job '{job_name}' failed.")

            time.sleep(poll_interval)

    def remove_job(self, name):
        """ Remove a job. This call is a blocking call, it will check and wait until the job is delete.
        All pods associated with that job will be deleted."""
        exists = True
        while exists:
            try:
                api = client.BatchV1Api()
                logger.info("looking up job {0}".format(name))
                job = api.read_namespaced_job(namespace=self.namespace, name=name)
                logger.info(f"job found... checking if being deleted")
                if job.metadata.deletion_timestamp:
                    logger.info(f"Job {name} has in {self.namespace} is being deleted.")
                    time.sleep(1)
                    continue
                else:
                    logger.info(f"Still alive, Issuing deletion of Job {name} in namespace {self.namespace}")
                    api.delete_namespaced_job(namespace=self.namespace, name=name)
                    time.sleep(1)
                    continue
            except kubernetes.client.exceptions.ApiException as e:
                if e.status == 404:
                    logger.info(f"Job {name} has been removed from namespace {self.namespace}.")
                    exists = False
                else:
                    logger.info(f"error occurred: {e}")
                    raise e
        logger.info(f"Job removed, removing pods associated with job - {name}")
        # Remove all related pods
        self.remove_pods(name)

    def remove_pod_with_wait(self, pod_name):
        exists = True
        while exists:
            try:
                v1 = client.CoreV1Api()

                pod = v1.read_namespaced_pod(name=pod_name, namespace=self.namespace)
                # Check if deletionTimestamp is set
                if pod.metadata.deletion_timestamp:
                    logger.info(f"Pod {pod_name} in namespace {self.namespace} is being deleted.")
                    time.sleep(1)
                    continue
                else:
                    logger.info(f"Issuing delete for Pod {pod_name} in namespace {self.namespace}")
                    v1.delete_namespaced_pod(namespace=self.namespace, name=pod_name)
                    time.sleep(1)
                    continue
            except kubernetes.client.exceptions.ApiException as e:
                if e.status == 404:
                    print(f"Pod {pod_name} has been removed from namespace {self.namespace}.")
                    exists = False
                else:
                    print(f"An error occurred: {e}")

    def remove_pods(self, job_name):
        result = client.CoreV1Api().list_namespaced_pod(namespace=self.namespace,
                                                        label_selector=f"job-name={job_name}")

        for pod in result.items:
            self.remove_pod_with_wait(pod.metadata.name)

