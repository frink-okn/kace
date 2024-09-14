import yaml
from jinja2 import Environment, FileSystemLoader
import os
from kubernetes import client, config
from kubernetes.client.rest import ApiException
from log_util import LoggingUtil


logger = LoggingUtil.init_logging("fuseki-k8s-man")


FUSEKI_TEMPLATE_DIR = (
        os.path.dirname(os.path.realpath(__file__)) +
        os.path.join(os.path.sep + "templates", "fuseki")
)

TEMPLATES = Environment(
    loader=FileSystemLoader(FUSEKI_TEMPLATE_DIR),
    trim_blocks=True,
    lstrip_blocks=True
)

config.load_incluster_config()


def get_config_map(kg_name: str):
    config_map_template = TEMPLATES.get_template("config-map.j2")
    return yaml.safe_load(config_map_template.render({
        "kg_name": kg_name
    }))


def get_ingress(kg_name: str):
    ingress_template = TEMPLATES.get_template("ingress.j2")
    return yaml.safe_load(ingress_template.render({
        "kg_name": kg_name
    }))


def get_deployment(kg_name: str, pvc_name: str):
    deployment_template = TEMPLATES.get_template("server-deployment.j2")
    return yaml.safe_load(deployment_template.render({
        "kg_name": kg_name,
        "pvc_name": pvc_name
    }))


def get_service(kg_name: str):
    services_template = TEMPLATES.get_template("service.j2")
    return yaml.safe_load(services_template.render({
        "kg_name": kg_name
    }))


def create_or_update_configmap_k8s(kg_name, namespace, annotations={}):
    config_map = get_config_map(kg_name)
    k8s_client = client.CoreV1Api()
    config_map_name = config_map["metadata"]["name"]
    raw_annotations = config_map["metadata"].get("annotations", {})
    raw_annotations.update(annotations)
    config_map["metadata"]["annotations"] = raw_annotations
    try:
        k8s_client.read_namespaced_config_map(name=config_map_name, namespace=namespace)
        k8s_client.patch_namespaced_config_map(name=config_map_name, namespace=namespace, body=config_map)
    except ApiException as e:
        if e.status == 404:
            k8s_client.create_namespaced_config_map(namespace=namespace, body=config_map)
        else:
            raise e


def create_or_update_k8s_service(kg_name, namespace, annotations={}):
    service_body = get_service(kg_name)
    k8s_client = client.CoreV1Api()
    service_name = service_body["metadata"]["name"]
    raw_annotations = service_body["metadata"].get("annotations", {})
    raw_annotations.update(annotations)
    service_body["metadata"]["annotations"] = raw_annotations
    try:
        k8s_client.read_namespaced_service(name=service_name, namespace=namespace)
        k8s_client.patch_namespaced_service(name=service_name, namespace=namespace, body=service_body)
    except ApiException as e:
        if e.status == 404:
            k8s_client.create_namespaced_service(namespace=namespace, body=service_body)
        else:
            raise e


def create_or_update_fuseki_deployment(kg_name, namespace, resources={}, pvc_name="", annotations={}):
    deployment_body = get_deployment(kg_name, pvc_name=pvc_name)
    deployment_name = deployment_body["metadata"]["name"]
    raw_annotations = deployment_body["metadata"].get("annotations", {})
    raw_annotations.update(annotations)
    deployment_body["metadata"]["annotations"] = raw_annotations
    deployment_body["spec"]["template"]["metadata"]["annotations"] = raw_annotations
    # update resource
    if resources:
        deployment_body["spec"]["template"]["spec"]["containers"][0]["resources"] = resources
    k8s_client = client.AppsV1Api()
    deployment_name = deployment_body["metadata"]["name"]
    try:
        k8s_client.read_namespaced_deployment(name=deployment_name, namespace=namespace)
        k8s_client.patch_namespaced_deployment(name=deployment_name, namespace=namespace, body=deployment_body)
    except ApiException as e:
        if e.status == 404:
            k8s_client.create_namespaced_deployment(namespace=namespace, body=deployment_body)
        else:
            raise e


def create_or_update_ingress(kg_name, namespace, annotations: dict):
    ingress_body = get_ingress(kg_name)
    ingress_name = ingress_body["metadata"]["name"]
    # update annotations
    raw_annotations = ingress_body["metadata"].get("annotations", {})
    raw_annotations.update(annotations)
    ingress_body["metadata"]["annotations"] = raw_annotations


    k8s_client = client.NetworkingV1Api()
    try:
        k8s_client.read_namespaced_ingress(name=ingress_name, namespace=namespace)
        k8s_client.patch_namespaced_ingress(name=ingress_name, namespace=namespace, body=ingress_body)
    except ApiException as e:
        if e.status == 404:
            k8s_client.create_namespaced_ingress(namespace=namespace, body=ingress_body)
        else:
            raise e


def create_k8s_objects(kg_name: str, namespace="", cpu: int = 1, memory: str = "1Gi", pvc_name="", annotations: dict = {}):
    # create or update
    create_or_update_configmap_k8s(kg_name=kg_name, namespace=namespace, annotations=annotations)
    # service
    create_or_update_k8s_service(kg_name=kg_name, namespace=namespace, annotations=annotations)
    # deployment
    create_or_update_fuseki_deployment(kg_name=kg_name,
                                       namespace=namespace,
                                       resources={
                                           "requests": {"cpu": cpu, "memory":memory},
                                           "limits": {"cpu": cpu, "memory": memory}
                                       },
                                       pvc_name=pvc_name,
                                       annotations=annotations)
    # ingress
    create_or_update_ingress(kg_name=kg_name, namespace=namespace, annotations=annotations)


def delete_configmap_k8s(kg_name: str, namespace=""):
    logger.info(f"Deleting fuseki configmap for {kg_name}")
    api = client.CoreV1Api()
    config_map_body = get_config_map(kg_name)
    config_map_name = config_map_body["metadata"]["name"]
    try:
        api.delete_namespaced_config_map(name=config_map_name, namespace=namespace)
    except ApiException as e:
        if e.status == 404:
            logger.info("Config map not found for {}".format(config_map_name))
        else:
            raise e

def delete_service_k8s(kg_name: str, namespace=""):
    logger.info(f"Deleting fuseki service for {kg_name}")
    api = client.CoreV1Api()
    service_body = get_service(kg_name)
    service_name = service_body["metadata"]["name"]
    try:
        api.delete_namespaced_service(name=service_name, namespace=namespace)
    except ApiException as e:
        if e.status == 404:
            logger.info("Service {0} not found".format(service_name))
        else:
            raise e

def delete_deployment_k8s(kg_name: str, namespace=""):
    logger.info(f"Deleting fuseki deployment for {kg_name}")
    api = client.AppsV1Api()
    deployment_body = get_deployment(kg_name)
    deployment_name = deployment_body["metadata"]["name"]
    try:
        api.delete_namespaced_deployment(name=deployment_name, namespace=namespace)
    except ApiException as e:
        if e.status == 404:
            logger.info("Deployment {0} not found".format(deployment_name))
        else:
            raise e


def delete_ingress_k8s(kg_name: str, namespace=""):
    logger.info(f"Deleting fuseki ingress for {kg_name}")
    api = client.NetworkingV1Api()
    ingress_body = get_ingress(kg_name)
    ingress_name = ingress_body["metadata"]["name"]
    try:
        api.delete_namespaced_ingress(name=ingress_name, namespace=namespace)
    except ApiException as e:
        if e.status == 404:
            logger.info("Ingress {0} not found".format(ingress_name))
        else:
            raise e


def delete_k8s_objects(kg_name: str, namespace="" ):
    delete_configmap_k8s(kg_name=kg_name, namespace=namespace)
    delete_service_k8s(kg_name=kg_name, namespace=namespace)
    delete_deployment_k8s(kg_name=kg_name, namespace=namespace)
    delete_ingress_k8s(kg_name=kg_name, namespace=namespace)


if __name__ == "__main__":
    delete_k8s_objects("climatekg", "kebedey")




