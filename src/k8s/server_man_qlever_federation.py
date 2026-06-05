from typing import Dict, Any
from kubernetes import client
from kubernetes.client.rest import ApiException

from log_util import LoggingUtil
from k8s.server_man_qlever import QLeverServerDeploymentManager
from config import config as app_config

logger = LoggingUtil.init_logging("qlever-federation-k8s-man")


class QLeverFederationServerDeploymentManager(QLeverServerDeploymentManager):
    """Manager for the federated QLever endpoint at `/federation`.

    Differs from the per-KG manager in two ways:
      * does not create or prune PVCs — the index PVC is produced by
        QLeverIndexWorkflow and passed in via `parameters["pvc_name"]`.
      * uses fixed K8s object names (`frink-federation-qlever-server`,
        `frink-federation-qlever-route`, etc.) so the Service stays stable
        across build rollovers and the HTTPRoute backendRef never changes.
    """

    def __init__(self, templates_dir, namespace):
        super().__init__(templates_dir, namespace, use_private_pvc=False)

    def create_all(self, parameters: Dict[str, Any], annotations: Dict[str, str] = None, resources: Dict[str, Any] = None) -> None:
        if app_config.networking_mode == "gateway":
            self.create_or_update_httproute(parameters=parameters, annotations=annotations)
            self.create_or_update_healthcheck(parameters=parameters, annotations=annotations)
            self.create_or_update_backend_policy(parameters=parameters, annotations=annotations)
        else:
            self.create_or_update_ingress(parameters=parameters, annotations=annotations)
        self.create_or_update_service(parameters=parameters, annotations=annotations)
        self.create_or_update_deployment(parameters=parameters, annotations=annotations, resources=resources)

    def are_all_services_running(self, parameters: Dict[str, Any], annotations: Dict[str, Any]) -> bool:
        service_name = self.get_service(parameters)["metadata"]["name"]
        deployment_name = self.get_deployment(parameters)["metadata"]["name"]
        return (self.is_service_running(service_name, annotations=annotations) and
                self.is_deployment_running(deployment_name, annotations=annotations))

    def wait_for_deployment_observed_generation(self, deployment_name: str, timeout_s: int = 600) -> None:
        """After a patch that swaps PVC claimName the Recreate strategy tears
        down the old pod before the new one starts. The parent helper waits
        on `available_replicas == replicas`, but during the gap that check
        can flap. This is a thin extra guard: poll the Deployment until the
        controller has observed the new generation."""
        import time
        api = client.AppsV1Api()
        deadline = time.time() + timeout_s
        target_generation = None
        while time.time() < deadline:
            try:
                d = api.read_namespaced_deployment(name=deployment_name, namespace=self.namespace)
                if target_generation is None:
                    target_generation = d.metadata.generation
                if d.status.observed_generation and d.status.observed_generation >= target_generation:
                    return
            except ApiException as e:
                logger.warning(f"Deployment read failed (will retry): {e}")
            time.sleep(2)
        raise TimeoutError(f"Deployment {deployment_name} did not observe new generation within {timeout_s}s")
