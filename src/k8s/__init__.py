import os
from k8s.server_man import ServerDeploymentManager
from k8s.server_man_ldf import LDFServerDeploymentMananger
from config import config as app_config
from k8s.podman import JobMan

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

ldf_server_manager = LDFServerDeploymentMananger(
    templates_dir=LDF_TEMPLATE_DIR,
    namespace=app_config.k8s_namespace
)

