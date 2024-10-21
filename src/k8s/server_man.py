from typing import Dict, Any

import yaml
from jinja2 import Environment, FileSystemLoader
import os
from kubernetes import client, config
from kubernetes.client.rest import ApiException
from log_util import LoggingUtil
from config import config as app_config


logger = LoggingUtil.init_logging("fuseki-k8s-man")


config.load_incluster_config()


class ServerDeploymentManager:
    def __init__(self, templates_dir, namespace):
        logger.info("Loading templates from {}".format(templates_dir))
        self.templates = Environment(
            loader=FileSystemLoader(templates_dir),
            trim_blocks=True,
            lstrip_blocks=True
        )
        self.server_host_name = app_config.frink_address
        self.pvc_name = app_config.shared_pvc_name
        self.namespace = namespace

    def get_config_map(self, parameters: Dict[str, Any]) -> Dict:
        config_map_template = self.templates.get_template("config-map.j2")
        return yaml.safe_load(config_map_template.render(parameters))

    def get_ingress(self, parameters: Dict[str, Any]) -> Dict:
        ingress_template = self.templates.get_template("ingress.j2")
        parameters.update({
            "host_name": self.server_host_name
        })
        return yaml.safe_load(ingress_template.render(parameters))

    def get_deployment(self, parameters: Dict[str, Any]) -> Dict:
        deployment_template = self.templates.get_template("server-deployment.j2")
        parameters.update({
            "pvc_name": self.pvc_name
        })
        return yaml.safe_load(deployment_template.render(parameters))

    def get_service(self, parameters: Dict[str, Any]) -> Dict:
        services_template = self.templates.get_template("service.j2")
        return yaml.safe_load(services_template.render(parameters))

    def create_or_update_configmap_k8s(self, parameters: Dict[str, Any], annotations: Dict[str, str] = None) -> None:
        config_map = self.get_config_map(parameters)
        k8s_client = client.CoreV1Api()
        config_map_name = config_map["metadata"]["name"]
        raw_annotations = config_map["metadata"].get("annotations", {})
        if annotations:
            raw_annotations.update(annotations)
            config_map["metadata"]["annotations"] = raw_annotations
        try:
            k8s_client.read_namespaced_config_map(name=config_map_name, namespace=self.namespace)
            k8s_client.patch_namespaced_config_map(name=config_map_name, namespace=self.namespace, body=config_map)
        except ApiException as e:
            if e.status == 404:
                k8s_client.create_namespaced_config_map(namespace=self.namespace, body=config_map)
            else:
                raise e

    def create_or_update_service(self, parameters: Dict[str, Any], annotations: Dict[str, str] = None) -> None:
        service_body = self.get_service(parameters)
        k8s_client = client.CoreV1Api()
        service_name = service_body["metadata"]["name"]
        if annotations:
            raw_annotations = service_body["metadata"].get("annotations", {})
            raw_annotations.update(annotations)
            service_body["metadata"]["annotations"] = raw_annotations
        try:
            k8s_client.read_namespaced_service(name=service_name, namespace=self.namespace)
            k8s_client.patch_namespaced_service(name=service_name, namespace=self.namespace, body=service_body)
        except ApiException as e:
            if e.status == 404:
                k8s_client.create_namespaced_service(namespace=self.namespace, body=service_body)
            else:
                raise e

    def create_or_update_deployment(self, parameters: Dict[str, Any], annotations: Dict[str, str] = None,
                                    resources: Dict[str, Any] = None) -> None:

        deployment_body = self.get_deployment(parameters)
        deployment_name = deployment_body["metadata"]["name"]
        if annotations:
            raw_annotations = deployment_body["metadata"].get("annotations", {})
            raw_annotations.update(annotations)
            deployment_body["metadata"]["annotations"] = raw_annotations
            deployment_body["spec"]["template"]["metadata"]["annotations"] = raw_annotations
        # update resource
        if resources:
            deployment_body["spec"]["template"]["spec"]["containers"][0]["resources"] = resources
        k8s_client = client.AppsV1Api()
        try:
            k8s_client.read_namespaced_deployment(name=deployment_name, namespace=self.namespace)
            k8s_client.patch_namespaced_deployment(name=deployment_name, namespace=self.namespace, body=deployment_body)
        except ApiException as e:
            if e.status == 404:
                k8s_client.create_namespaced_deployment(namespace=self.namespace, body=deployment_body)
            else:
                raise e

    def create_or_update_ingress(self, parameters: Dict[str, Any], annotations: Dict[str, str] = None) -> None:
        ingress_body = self.get_ingress(parameters)
        ingress_name = ingress_body["metadata"]["name"]
        # update annotations
        if annotations:
            raw_annotations = ingress_body["metadata"].get("annotations", {})
            raw_annotations.update(annotations)
            ingress_body["metadata"]["annotations"] = raw_annotations

        k8s_client = client.NetworkingV1Api()
        try:
            k8s_client.read_namespaced_ingress(name=ingress_name, namespace=self.namespace)
            k8s_client.patch_namespaced_ingress(name=ingress_name, namespace=self.namespace, body=ingress_body)
        except ApiException as e:
            if e.status == 404:
                k8s_client.create_namespaced_ingress(namespace=self.namespace, body=ingress_body)
            else:
                raise e

    def create_all(self, parameters: Dict[str, Any], annotations: Dict[str, str] = None, resources: Dict[str, Any] = None) -> None:
        self.create_or_update_ingress(parameters=parameters, annotations=annotations)
        self.create_or_update_configmap_k8s(parameters=parameters, annotations=annotations)
        self.create_or_update_service(parameters=parameters, annotations=annotations)
        self.create_or_update_deployment(parameters=parameters,
                                         annotations=annotations,
                                         resources=resources)

    def delete_configmap_k8s(self, parameters: Dict[str, Any]) -> None:
        api = client.CoreV1Api()
        config_map_body = self.get_config_map(parameters)
        config_map_name = config_map_body["metadata"]["name"]
        logger.info(f"Deleting configmap : {config_map_name}")
        try:
            api.delete_namespaced_config_map(name=config_map_name, namespace=self.namespace)
        except ApiException as e:
            if e.status == 404:
                logger.info("Config map not found : {}".format(config_map_name))
            else:
                raise e

    def delete_service_k8s(self, parameters: Dict[str, Any]) -> None:
        api = client.CoreV1Api()
        service_body = self.get_service(parameters)
        service_name = service_body["metadata"]["name"]
        logger.info(f"Deleting service : {service_name}")
        try:
            api.delete_namespaced_service(name=service_name, namespace=self.namespace)
        except ApiException as e:
            if e.status == 404:
                logger.info("Service not found : {0}".format(service_name))
            else:
                raise e

    def delete_deployment_k8s(self, parameters: Dict[str, Any]) -> None:
        api = client.AppsV1Api()
        deployment_body = self.get_deployment(parameters)
        deployment_name = deployment_body["metadata"]["name"]
        logger.info(f"Deleting  deployment : {deployment_name}")
        try:
            api.delete_namespaced_deployment(name=deployment_name, namespace=self.namespace)
        except ApiException as e:
            if e.status == 404:
                logger.info("Deployment not found : {0}".format(deployment_name))
            else:
                raise e

    def delete_ingress_k8s(self, parameters: Dict[str, Any]) -> None:
        api = client.NetworkingV1Api()
        ingress_body = self.get_ingress(parameters)
        ingress_name = ingress_body["metadata"]["name"]
        logger.info(f"Deleting ingress : {ingress_name}")
        try:
            api.delete_namespaced_ingress(name=ingress_name, namespace=self.namespace)
        except ApiException as e:
            if e.status == 404:
                logger.info("Ingress not found: {0}".format(ingress_name))
            else:
                raise e

    def delete_k8s_objects(self, parameters: Dict[str, Any]) -> None:
        self.delete_configmap_k8s(parameters)
        self.delete_service_k8s(parameters)
        self.delete_deployment_k8s(parameters)
        self.delete_ingress_k8s(parameters)


FUSEKI_TEMPLATE_DIR = (
        os.path.dirname(os.path.realpath(__file__)) +
        os.path.join(os.path.sep + "templates", "fuseki")
)
FEDERATION_TEMPLATE_DIR = (
        os.path.dirname(os.path.realpath(__file__)) +
        os.path.join(os.path.sep + "templates", "federation")
)
LDF_TEMPLATE_DIR = (
        os.path.dirname(os.path.realpath(__file__)) +
        os.path.join(os.path.sep + "templates", "ldf")
)

fuseki_server_manager = ServerDeploymentManager(
    templates_dir=FUSEKI_TEMPLATE_DIR,
    namespace=app_config.k8s_namespace
)

federation_server_manager = ServerDeploymentManager(
    templates_dir=FEDERATION_TEMPLATE_DIR,
    namespace=app_config.k8s_namespace
)

ldf_server_manager = ServerDeploymentManager(
    templates_dir=LDF_TEMPLATE_DIR,
    namespace=app_config.k8s_namespace
)





