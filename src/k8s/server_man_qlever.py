from log_util import LoggingUtil
from k8s.server_man import ServerDeploymentManager
import yaml
from typing import Dict, Any
from kubernetes import client
from kubernetes.client.rest import ApiException
import os
from config import config as app_config

logger = LoggingUtil.init_logging("qlever-k8s-man")

class QLeverServerDeploymentManager(ServerDeploymentManager):
    def __init__(self, templates_dir, namespace, use_private_pvc: bool = True):
        super().__init__(templates_dir, namespace)
        self.use_private_pvc = use_private_pvc

    def get_deployment(self, parameters: Dict[str, Any]) -> Dict:
        deployment_template = self.templates.get_template("server-deployment.j2")
        if not self.use_private_pvc:
            parameters.update({"pvc_name": self.pvc_name})
        return yaml.safe_load(deployment_template.render(parameters))

    def get_pvc(self, parameters: Dict[str, Any]) -> Dict:
        pvc_template = self.templates.get_template("pvc.j2")
        return yaml.safe_load(pvc_template.render(parameters))

    def create_or_update_pvc(self, parameters: Dict[str, Any], annotations: Dict[str, str] = None) -> None:
        pvc_body = self.get_pvc(parameters)
        k8s_client = client.CoreV1Api()
        pvc_name = pvc_body["metadata"]["name"]

        if annotations:
            raw_annotations = pvc_body["metadata"].get("annotations", {})
            raw_annotations.update(annotations)
            pvc_body["metadata"]["annotations"] = raw_annotations

        try:
            k8s_client.read_namespaced_persistent_volume_claim(name=pvc_name, namespace=self.namespace)
            k8s_client.patch_namespaced_persistent_volume_claim(name=pvc_name, namespace=self.namespace, body=pvc_body)
            logger.info(f"Updated PVC {pvc_name}")
        except ApiException as e:
            if e.status == 404:
                k8s_client.create_namespaced_persistent_volume_claim(namespace=self.namespace, body=pvc_body)
                logger.info(f"Created PVC {pvc_name}")
            else:
                raise e

    def create_all(self, parameters: Dict[str, Any], annotations: Dict[str, str] = None, resources: Dict[str, Any] = None) -> None:
        """
        Deploy the Qlever Server.
        For Qlever, we must provision the dynamically created standalone PVC first.
        """
        if self.use_private_pvc:
            self.create_or_update_pvc(parameters=parameters, annotations=annotations)
        else:
            self.prune_old_deployments(parameters["kg_name"])
        if app_config.networking_mode == "gateway":
            self.create_or_update_httproute(parameters=parameters, annotations=annotations)
            self.create_or_update_healthcheck(parameters=parameters, annotations=annotations)
        else:
            self.create_or_update_ingress(parameters=parameters, annotations=annotations)
        self.create_or_update_service(parameters=parameters, annotations=annotations)
        self.create_or_update_deployment(parameters=parameters, annotations=annotations, resources=resources)

    def are_all_services_running(self, parameters: Dict[str, Any], annotations: Dict[str, Any]) -> bool:
        """
        Check if all services for QLever are running. Overridden to skip ConfigMap check since it doesn't exist.
        """
        service_name = self.get_service(parameters)["metadata"]["name"]
        deployment_name = self.get_deployment(parameters)["metadata"]["name"]

        return (self.is_service_running(service_name, annotations=annotations) and
                self.is_deployment_running(deployment_name, annotations=annotations))

    def prune_old_deployments(self, kg_name: str, keep_version: str = None) -> None:
        """
        Scan for and delete any PVCs for this kg_name that do NOT match keep_version.
        Pass keep_version=None to delete all private PVCs (e.g. when switching to shared PVC).
        """
        k8s_core = client.CoreV1Api()
        k8s_apps = client.AppsV1Api()

        label_selector = f"app=frink-{kg_name}-qlever-server"

        # Prune old PVCs
        try:
            pvcs = k8s_core.list_namespaced_persistent_volume_claim(namespace=self.namespace, label_selector=label_selector)
            for pvc in pvcs.items:
                version = pvc.metadata.annotations.get("version") if pvc.metadata.annotations else None
                if keep_version is None or version != keep_version:
                    logger.info(f"Pruning old PVC: {pvc.metadata.name}")
                    k8s_core.delete_namespaced_persistent_volume_claim(name=pvc.metadata.name, namespace=self.namespace)
        except Exception as e:
            logger.error(f"Failed to prune PVCs: {e}")

        # Prune old Deployments (Note: the selector matches the app, but since there is only one
        # deployment template Name matching frink-{kg_name}-qlever-server, the previous one was just
        # OVERWRITTEN. Thus, we only really manually manage the PVC lifecycle.)
        # The Deployment YAML uses a static name `frink-{{ kg_name }}-qlever-server`.
        # When applied, Kubernetes performs a rolling update automatically.
        # This keeps the Service and HTTPRoute perfectly stable.
        # The only leaked object over time is the PVC which we dynamically name and must delete.

if __name__ == "__main__":
    QLEVER_TEMPLATE_DIR = (
            os.path.dirname(os.path.realpath(__file__)) +
            os.path.join(os.path.sep + "templates", "qlever")
    )
    man = QLeverServerDeploymentManager(
        QLEVER_TEMPLATE_DIR, app_config.k8s_namespace
    )
    print("Test Initialization Complete")
