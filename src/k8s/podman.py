import time
import asyncio

import kubernetes.client.exceptions
from kubernetes import client, config, watch
import yaml
import os
from typing import Dict, List
from log_util import LoggingUtil
from config import config as app_conf
logger = LoggingUtil.init_logging(__name__)

mapping = {
    "nt-merge-job": os.path.dirname(os.path.realpath(__file__)) + os.path.join(os.path.sep + "templates", "nt-merge-job.yaml"),
    "spider-job": os.path.dirname(os.path.realpath(__file__)) + os.path.join(os.path.sep + "templates", "spider-client.yaml"),
    "neo4j-rdf-job": os.path.dirname(os.path.realpath(__file__)) + os.path.join(os.path.sep + "templates", "neo4j-rdf-job.yaml"),
    "neo4j-json-job": os.path.dirname(os.path.realpath(__file__)) + os.path.join(os.path.sep + "templates", "neo4j-json-job.yaml"),
    "documentation-job": os.path.dirname(os.path.realpath(__file__)) + os.path.join(os.path.sep + "templates", "documentation-job.yaml"),
    "qlever-index-job": os.path.dirname(os.path.realpath(__file__)) + os.path.join(os.path.sep + "templates", "qlever-index-job.yaml"),
    "void-job": os.path.dirname(os.path.realpath(__file__)) + os.path.join(os.path.sep + "templates", "void-description-job.yaml"),
    "hdtc-job": os.path.dirname(os.path.realpath(__file__)) + os.path.join(os.path.sep + "templates", "hdtc-conversion.yaml"),
    ## add other pods here
}

config.load_incluster_config()
# config.load_kube_config('/mnt/c/Users/kebedey/kubeconfig/kubeconfig-sterling-kebedey-kebedey')


_TOKEN_FILE = "/var/run/secrets/kubernetes.io/serviceaccount/token"


def _log_token_diag():
    """One-line diagnostic for the SA token file. Helps distinguish
    'no token' vs 'malformed token' vs 'rotated token' when k8s answers
    `system:anonymous` to an API call."""
    try:
        st = os.stat(_TOKEN_FILE)
        with open(_TOKEN_FILE) as fh:
            tok = fh.read().strip()
        looks_jwt = tok.count(".") == 2 and len(tok) > 50
        logger.info(
            f"SA token diag: size={st.st_size}B mtime={st.st_mtime} "
            f"jwt_shape={looks_jwt} head={tok[:20]!r}"
        )
    except Exception as e:
        logger.error(f"SA token diag failed: {e}")


def _fresh_api_client():
    """Build a brand-new ApiClient with a Configuration loaded from disk.

    Two things we need to handle:

    1. GKE / k8s >= 1.24 rotate the projected SA token ~hourly. Older code
       called `load_incluster_config()` once at module import, so the cached
       token went stale and the client started hitting `system:anonymous`.
       Fix: build a fresh Configuration per call.

    2. Empirically, some versions of the kubernetes Python client (we pin
       nothing in requirements.txt) fail to attach the Bearer header from
       `Configuration.api_key` even though `load_incluster_config()` wrote
       the right value. Observable symptom: a curl with the same token
       authenticates correctly, but `client.CoreV1Api()` calls return
       `system:anonymous`. Fix: after the loader runs, ALSO read the token
       file ourselves and overwrite both `api_key` and the per-host default
       headers with an explicit `Authorization: bearer ...`. This bypasses
       any internal refresh-hook bookkeeping the client may have gotten
       wrong.
    """
    cfg = client.Configuration()
    try:
        config.load_incluster_config(client_configuration=cfg)
    except Exception as e:
        logger.error(f"load_incluster_config failed: {e}")
        _log_token_diag()
        raise

    # Manual bearer-header override — see (2) above.
    try:
        with open(_TOKEN_FILE) as fh:
            token = fh.read().strip()
        if token:
            cfg.api_key = {"authorization": f"bearer {token}"}
            cfg.api_key_prefix = {}
            api_client = client.ApiClient(configuration=cfg)
            api_client.set_default_header("Authorization", f"Bearer {token}")
            return api_client
        else:
            logger.warning("SA token file is empty; falling back to client default auth.")
    except Exception as e:
        logger.warning(f"Manual token attach failed, falling back to client default: {e}")

    return client.ApiClient(configuration=cfg)


def _core_v1():
    return client.CoreV1Api(api_client=_fresh_api_client())


def _batch_v1():
    return client.BatchV1Api(api_client=_fresh_api_client())


# Kept for source-compat with prior fix; now a no-op since callers use
# the _core_v1 / _batch_v1 helpers above.
def _reload_k8s_auth():
    pass


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

    def ensure_configmap(self, name, data: Dict[str, str]):
        _reload_k8s_auth()
        core_v1 = _core_v1()
        configmap_name = f"{name}-config"
        metadata = client.V1ObjectMeta(
            name=configmap_name,
            namespace=self.namespace,
        )
        body = client.V1ConfigMap(
            api_version="v1",
            kind="ConfigMap",
            metadata=metadata,
            data=data
        )
        try:
            core_v1.read_namespaced_config_map(name=configmap_name, namespace=self.namespace)
            # update
            logger.info(f"ConfigMap {configmap_name} exists, patching...")
            core_v1.patch_namespaced_config_map(name=configmap_name, namespace=self.namespace, body=body)
        except client.exceptions.ApiException as e:
            if e.status == 404:
                logger.info(f"Creating ConfigMap {configmap_name}...")
                core_v1.create_namespaced_config_map(namespace=self.namespace, body=body)
            else:
                logger.error(f"Failed to ensure ConfigMap {configmap_name}: {e}")
                raise e

    @staticmethod
    def init_job_object(name: str, image: str, command: List[str], configmap: Dict[str, str] = None) -> client.V1Job:
        # create the pod
        volumes = [
            client.V1Volume(
                name="data",
                persistent_volume_claim=client.V1PersistentVolumeClaimVolumeSource(
                    claim_name=app_conf.local_pvc_name
                )
            )
        ]
        
        container_mounts = []
        if configmap:
             volumes.append(client.V1Volume(
                 name="config-volume",
                 config_map=client.V1ConfigMapVolumeSource(
                     name=f"{name}-config"
                 )
             ))
             for path, content in configmap.items():
                 filename = os.path.basename(path)
                 container_mounts.append(client.V1VolumeMount(
                     name="config-volume",
                     mount_path=path,
                     sub_path=filename
                 ))

        container = client.V1Container(
            name=name,
            image=image,
            command=command,
            tty=True,
            stdin=True,
        )
        if container_mounts:
            container.volume_mounts = container_mounts

        return client.V1Job(
            api_version="batch/v1",
            kind="Job",
            metadata=client.V1ObjectMeta(name=name),
            spec=client.V1JobSpec(
                template=client.V1PodTemplateSpec(
                    # ponytail: block GKE Autopilot/cluster-autoscaler from evicting
                    # long-running batch pods (e.g. multi-day qlever index build) during
                    # node consolidation. Autopilot honors this for runs < 7d.
                    metadata=client.V1ObjectMeta(
                        annotations={"cluster-autoscaler.kubernetes.io/safe-to-evict": "false"}
                    ),
                    spec=client.V1PodSpec(
                        containers=[container],
                        restart_policy="Never",
                        volumes=volumes
                    )
                ),
                backoff_limit=0,
            ),
        )

    def run_job(self, job_type, job_name, repo: str, branch: str, command: List[str] = None, args: List[str] = None,
                resources=None, env_vars=None, additional_volume_mounts=None,
                image: str = None, extra_pvcs: List[Dict] = None,
                read_only_default_mount: bool = False,
                pod_security_context: Dict = None,
                configmap_overrides: Dict[str, str] = None):
        """
        Args:
            extra_pvcs: list of dicts with keys
                {"name": volume name (k8s id),
                 "claim": PVC claim name,
                 "mount_path": container mount path,
                 "read_only": bool (optional, default False)}
                Volumes are appended to the pod and mounts to container[0].
            image: override the container image (e.g. for the qlever-index job
                whose image is config-driven, not template-driven).
            read_only_default_mount: when True, mount the default `data`
                volume at /mnt/repo as read-only. Used by the federated
                qlever-index job (it must not write to source files).
            configmap_overrides: {configmap_path: content} — replace the
                content of a configmap file declared in the job template
                (matched by its full path key, e.g.
                "/qlever/frink-qlever.settings.json"). Lets callers supply
                config-driven file contents without baking them into the
                static template.
        """
        _reload_k8s_auth()
        if env_vars is None:
            env_vars = dict()
        job: kubernetes.client.V1Job = self.job_objects[job_type]
        # override default name, by default its named as the job-type
        job.metadata.name = job_name
        # override command and args
        logger.info("job man recieved job {} - {}".format(job_type, job_name))
        pod_template: kubernetes.client.V1PodSpec = job.spec.template.spec
        # pod_template.containers[0].command = ["/bin/sh"]
        # pod_template.containers[0].stdin_open = True
        # pod_template.containers[0].tty = True
        if command:
            pod_template.containers[0].command = command
        if args:
            logger.info("pod args {}".format(args))
            pod_template.containers[0].args = args
        if resources:
            # K8s API requires quantities as strings; coerce in case
            # values arrive as integers (e.g. after JSON round-trip via Temporal)
            for section in ("requests", "limits"):
                if section in resources:
                    resources[section] = {k: str(v) for k, v in resources[section].items()}
            resources = client.V1ResourceRequirements(**resources)
            pod_template.containers[0].resources = resources
        if env_vars:
            env = [kubernetes.client.V1EnvVar(name=key, value=value) for key, value in env_vars.items()]
            pod_template.containers[0].env = env
        if image:
            pod_template.containers[0].image = image
        # @TODO not all job containers need this but ok ...
        volume_mounts = [
            client.V1VolumeMount(
                name="data",
                mount_path="/mnt/repo",
                read_only=read_only_default_mount,
                # sub_path=f"{repo}/{branch}"
            )
        ]
        
        # Check for configmap in job config and ensure it exists
        job_config = self.job_configs.get(job_type)
        if job_config and "configmap" in job_config:
            cm_data = {}
            for path, content in job_config["configmap"].items():
                filename = os.path.basename(path)
                if configmap_overrides and path in configmap_overrides:
                    content = configmap_overrides[path]
                cm_data[filename] = content
            self.ensure_configmap(job_type, cm_data)
            
            # The mounts inside the container definition in init_job_object are for the base definition. 
            # When we access job.spec.template.spec.containers[0], we need to make sure we preserve existing mounts 
            # (which presumably includes the configmap mounts we added in init_job_object)
            if pod_template.containers[0].volume_mounts:
                volume_mounts.extend(pod_template.containers[0].volume_mounts)

        if additional_volume_mounts:
            for v in additional_volume_mounts:
                volume_mounts.append(client.V1VolumeMount(
                    name=v[0],
                    mount_path=v[1]
                ))

        if extra_pvcs:
            existing_volumes = {vol.name for vol in pod_template.volumes or []}
            for spec in extra_pvcs:
                vol_name   = spec["name"]
                claim_name = spec["claim"]
                mount_path = spec["mount_path"]
                read_only  = bool(spec.get("read_only", False))
                if vol_name not in existing_volumes:
                    pod_template.volumes = (pod_template.volumes or []) + [
                        client.V1Volume(
                            name=vol_name,
                            persistent_volume_claim=client.V1PersistentVolumeClaimVolumeSource(
                                claim_name=claim_name
                            ),
                        )
                    ]
                    existing_volumes.add(vol_name)
                volume_mounts.append(client.V1VolumeMount(
                    name=vol_name,
                    mount_path=mount_path,
                    read_only=read_only,
                ))

        # De-duplicate mounts by mount_path just in case
        unique_mounts = {vm.mount_path: vm for vm in volume_mounts}
        pod_template.containers[0].volume_mounts = list(unique_mounts.values())

        if pod_security_context:
            pod_template.security_context = client.V1PodSecurityContext(**pod_security_context)

        api = _batch_v1()
        logger.info(f"removing previous jobs {job_name} ")
        self.remove_job(job_name)
        return api.create_namespaced_job(namespace=self.namespace, body=job)

    def _get_pod_logs_for_job(self, job_name, tail_lines=100):
        """Fetch logs and status from all pods belonging to a job.

        Returns a formatted string with pod status, exit codes, and tail logs.
        """
        _reload_k8s_auth()
        core_v1 = _core_v1()
        output_parts = []
        try:
            pods = core_v1.list_namespaced_pod(
                namespace=self.namespace,
                label_selector=f"job-name={job_name}"
            )
            for pod in pods.items:
                pod_name = pod.metadata.name
                phase = pod.status.phase
                part = [f"\n--- Pod: {pod_name} (phase: {phase}) ---"]

                # Container exit codes and reasons
                if pod.status.container_statuses:
                    for cs in pod.status.container_statuses:
                        terminated = cs.state.terminated
                        if terminated:
                            part.append(
                                f"  Container '{cs.name}': exit_code={terminated.exit_code}, "
                                f"reason={terminated.reason}, message={terminated.message}"
                            )
                        elif cs.state.waiting:
                            part.append(
                                f"  Container '{cs.name}': waiting, "
                                f"reason={cs.state.waiting.reason}, message={cs.state.waiting.message}"
                            )

                # Pod logs
                try:
                    logs = core_v1.read_namespaced_pod_log(
                        name=pod_name,
                        namespace=self.namespace,
                        tail_lines=tail_lines
                    )
                    part.append(f"  Logs (last {tail_lines} lines):\n{logs}")
                except client.exceptions.ApiException:
                    part.append("  Logs: <unavailable>")

                output_parts.append("\n".join(part))

        except client.exceptions.ApiException as e:
            output_parts.append(f"Failed to fetch pod info for job '{job_name}': {e}")

        return "\n".join(output_parts) if output_parts else "<no pods found>"

    def watch_job(self, job_name, poll_interval=5):
        """
        Watch the Job's status until it completes (succeeds or fails).
        This function will continue polling indefinitely until the Job reaches a terminal state.

        Args:
            job_name (str): The name of the Job.
            poll_interval (int): Seconds to wait between successive status checks.

        Raises:
            Exception: If the Job fails. Includes pod logs and exit codes.
        """
        while True:
            _reload_k8s_auth()
            batch_v1 = _batch_v1()
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
            if failed > backoff_limit:
                pod_info = self._get_pod_logs_for_job(job_name)
                logger.error(f"Job '{job_name}' failed after {failed} attempts.\n{pod_info}")
                raise Exception(f"Job '{job_name}' failed after {failed} attempts.\n{pod_info}")

            time.sleep(poll_interval)

    async def async_watch_job(self, job_name, poll_interval=5):
        """
        Async version of watch_job. Uses asyncio.sleep instead of time.sleep
        so it can be awaited without blocking the event loop.

        Args:
            job_name (str): The name of the Job.
            poll_interval (int): Seconds to wait between successive status checks.

        Raises:
            Exception: If the Job fails. Includes pod logs and exit codes.
        """
        while True:
            _reload_k8s_auth()
            batch_v1 = _batch_v1()
            try:
                job = batch_v1.read_namespaced_job(name=job_name, namespace=self.namespace)
            except client.exceptions.ApiException as e:
                logger.error(f"Failed to fetch Job '{job_name}': {e}")
                raise Exception(f"Failed to fetch Job '{job_name}': {e} , likely the job was never created.")

            succeeded = job.status.succeeded or 0
            failed = job.status.failed or 0
            backoff_limit = job.spec.backoff_limit if job.spec.backoff_limit is not None else 6

            logger.info(f"Job '{job_name}' status: succeeded={succeeded}, failed={failed}")

            if succeeded > 0:
                logger.info(f"Job '{job_name}' completed successfully.")
                return

            if failed > backoff_limit:
                pod_info = self._get_pod_logs_for_job(job_name)
                logger.error(f"Job '{job_name}' failed after {failed} attempts.\n{pod_info}")
                raise Exception(f"Job '{job_name}' failed after {failed} attempts.\n{pod_info}")

            await asyncio.sleep(poll_interval)


    def remove_job(self, name):
        """ Remove a job. This call is a blocking call, it will check and wait until the job is delete.
        All pods associated with that job will be deleted."""
        exists = True
        while exists:
            try:
                _reload_k8s_auth()
                api = _batch_v1()
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
                _reload_k8s_auth()
                v1 = _core_v1()

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
        _reload_k8s_auth()
        result = _core_v1().list_namespaced_pod(namespace=self.namespace,
                                                        label_selector=f"job-name={job_name}")

        for pod in result.items:
            self.remove_pod_with_wait(pod.metadata.name)

