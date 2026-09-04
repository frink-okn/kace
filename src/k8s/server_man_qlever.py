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

    def get_backend_policy(self, parameters: Dict[str, Any]) -> Dict:
        tmpl = self.templates.get_template("backend-policy.j2")
        return yaml.safe_load(tmpl.render(parameters))

    def create_or_update_backend_policy(self, parameters: Dict[str, Any], annotations: Dict[str, str] = None) -> None:
        """Apply a GCPBackendPolicy so the GKE gateway uses long backend
        timeouts (default 3600s) + connection draining for long-running SPARQL
        queries, instead of dropping the connection with a 504."""
        body = self.get_backend_policy(parameters)
        name = body["metadata"]["name"]
        if annotations:
            raw = body["metadata"].get("annotations", {})
            raw.update(annotations)
            body["metadata"]["annotations"] = raw

        k8s_client = self._custom()
        group = "networking.gke.io"
        version = "v1"
        plural = "gcpbackendpolicies"
        try:
            k8s_client.get_namespaced_custom_object(
                group=group, version=version, namespace=self.namespace, plural=plural, name=name
            )
            k8s_client.patch_namespaced_custom_object(
                group=group, version=version, namespace=self.namespace, plural=plural, name=name, body=body
            )
        except ApiException as e:
            if e.status == 404:
                k8s_client.create_namespaced_custom_object(
                    group=group, version=version, namespace=self.namespace, plural=plural, body=body
                )
            else:
                raise e

    def get_httpscaledobject(self, parameters: Dict[str, Any]) -> Dict:
        tmpl = self.templates.get_template("httpscaledobject.j2")
        return yaml.safe_load(tmpl.render(parameters))

    def create_or_update_httpscaledobject(self, parameters: Dict[str, Any], annotations: Dict[str, str] = None) -> None:
        """Hand this KG's replica count to KEDA (min 0, max 1).

        The per-KG servers answer ~2 organic requests a day between them, so they
        run at zero and the interceptor named in httproute.j2 wakes one on demand.
        Without this object the interceptor has no route for the Host header that
        route rewrites to, and every request for this KG gets a 404."""
        body = self.get_httpscaledobject(parameters)
        name = body["metadata"]["name"]
        if annotations:
            raw = body["metadata"].get("annotations", {})
            raw.update(annotations)
            body["metadata"]["annotations"] = raw

        k8s_client = self._custom()
        group = "http.keda.sh"
        version = "v1alpha1"
        plural = "httpscaledobjects"
        try:
            k8s_client.get_namespaced_custom_object(
                group=group, version=version, namespace=self.namespace, plural=plural, name=name
            )
            k8s_client.patch_namespaced_custom_object(
                group=group, version=version, namespace=self.namespace, plural=plural, name=name, body=body
            )
        except ApiException as e:
            if e.status == 404:
                k8s_client.create_namespaced_custom_object(
                    group=group, version=version, namespace=self.namespace, plural=plural, body=body
                )
            else:
                raise e

    def create_or_update_pvc(self, parameters: Dict[str, Any], annotations: Dict[str, str] = None) -> None:
        pvc_body = self.get_pvc(parameters)
        k8s_client = self._core()
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
            self.create_or_update_backend_policy(parameters=parameters, annotations=annotations)
        else:
            self.create_or_update_ingress(parameters=parameters, annotations=annotations)
        self.create_or_update_service(parameters=parameters, annotations=annotations)
        self.create_or_update_deployment(parameters=parameters, annotations=annotations, resources=resources)
        if app_config.networking_mode == "gateway":
            # After the Deployment, so KEDA finds its scale target on the first
            # reconcile instead of reporting a missing one and retrying. The
            # window where the route points at an interceptor that has no entry
            # for this KG yet is sub-second, and this KG has no traffic yet.
            self.create_or_update_httpscaledobject(parameters=parameters, annotations=annotations)

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
        k8s_core = self._core()
        k8s_apps = self._apps()

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
