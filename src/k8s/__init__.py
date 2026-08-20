import os
from k8s.server_man import ServerDeploymentManager
from k8s.server_man_ldf import LDFServerDeploymentMananger
from k8s.server_man_qlever import QLeverServerDeploymentManager
from k8s.server_man_qlever_federation import QLeverFederationServerDeploymentManager
from config import config as app_config
from k8s import clients
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
QLEVER_TEMPLATE_DIR = (
        os.path.dirname(os.path.realpath(__file__)) +
        os.path.join(os.path.sep + "templates", "qlever")
)
QLEVER_FEDERATION_TEMPLATE_DIR = (
        os.path.dirname(os.path.realpath(__file__)) +
        os.path.join(os.path.sep + "templates", "qlever-federation")
)

fuseki_server_manager = ServerDeploymentManager(
    templates_dir=FUSEKI_TEMPLATE_DIR,
    namespace=clients.namespace(clients.REMOTE)
)

federation_server_manager = ServerDeploymentManager(
    templates_dir=FEDERATION_TEMPLATE_DIR,
    namespace=clients.namespace(clients.REMOTE)
)

ldf_server_manager = LDFServerDeploymentMananger(
    templates_dir=LDF_TEMPLATE_DIR,
    namespace=clients.namespace(clients.REMOTE)
)

qlever_server_manager = QLeverServerDeploymentManager(
    templates_dir=QLEVER_TEMPLATE_DIR,
    namespace=clients.namespace(clients.REMOTE),
    use_private_pvc=app_config.qlever_use_private_pvc
)

qlever_federation_server_manager = QLeverFederationServerDeploymentManager(
    templates_dir=QLEVER_FEDERATION_TEMPLATE_DIR,
    namespace=clients.namespace(clients.REMOTE),
)

