"""K8s PVC manager for per-build qlever federated index volumes.

Each QLeverIndexWorkflow run allocates a new RWO PVC named
``{prefix}{build_id}`` (e.g. ``kace-qlever-index-20260522-153012``). The PVC
is keyed by build_id so that:

  * downstream server-rollover can target a specific build,
  * GC can identify orphans (anything not currently serving or previous),
  * a 24h grace period applies to the previous build before delete.
"""
from datetime import datetime, timedelta, timezone
from typing import Dict, List, Optional
from kubernetes import client, config as k8s_config
from kubernetes.client.rest import ApiException
from config import config as app_config


def pvc_name(build_id: str) -> str:
    return f"{app_config.qlever_index_pvc_prefix}{build_id}"


def _reload_k8s_auth():
    # legacy no-op
    pass


def _fresh_api_client():
    cfg = client.Configuration()
    k8s_config.load_incluster_config(client_configuration=cfg)
    return client.ApiClient(configuration=cfg)


def _api() -> client.CoreV1Api:
    return client.CoreV1Api(api_client=_fresh_api_client())


def create_index_pvc(build_id: str, image: str) -> str:
    """Create the per-build output PVC. Idempotent: returns silently if it
    already exists. Annotates with the qlever image used so rollback can
    refuse to mount an index built by an incompatible binary."""
    name = pvc_name(build_id)
    namespace = app_config.k8s_namespace
    body = client.V1PersistentVolumeClaim(
        metadata=client.V1ObjectMeta(
            name=name,
            annotations={
                "kace.frink/qlever-image": image,
                "kace.frink/build-id":     build_id,
                "kace.frink/created-at":   datetime.now(timezone.utc).isoformat(),
            },
            labels={
                "app.kubernetes.io/managed-by": "kace",
                "kace.frink/role":              "qlever-index",
            },
        ),
        spec=client.V1PersistentVolumeClaimSpec(
            access_modes=["ReadWriteOnce"],
            storage_class_name=app_config.qlever_index_pvc_storage_class,
            resources=client.V1ResourceRequirements(
                requests={"storage": app_config.qlever_index_pvc_size},
            ),
        ),
    )
    api = _api()
    try:
        api.read_namespaced_persistent_volume_claim(name=name, namespace=namespace)
        return name
    except ApiException as e:
        if e.status != 404:
            raise
    api.create_namespaced_persistent_volume_claim(namespace=namespace, body=body)
    return name


def list_index_pvcs() -> List[str]:
    """All PVCs in the namespace that carry the qlever-index role label."""
    namespace = app_config.k8s_namespace
    resp = _api().list_namespaced_persistent_volume_claim(
        namespace=namespace,
        label_selector="kace.frink/role=qlever-index",
    )
    return [item.metadata.name for item in resp.items]


def delete_pvc(name: str) -> None:
    namespace = app_config.k8s_namespace
    try:
        _api().delete_namespaced_persistent_volume_claim(name=name, namespace=namespace)
    except ApiException as e:
        if e.status != 404:
            raise


def gc_index_pvcs(state: Dict, now_iso: str) -> Dict[str, List[str]]:
    """Delete output PVCs that should no longer exist.

    Deletes:
      * orphans — any qlever-index PVC whose name is not the serving or
        previous build.
      * the previous build PVC if it was marked > qlever_index_previous_ttl_hours
        ago.

    Returns a report (deleted/retained) for observability.
    """
    prefix             = app_config.qlever_index_pvc_prefix
    ttl_hours          = app_config.qlever_index_previous_ttl_hours
    serving_build      = state.get("build_id_serving")
    previous_build     = state.get("build_id_previous")
    previous_marked_at = state.get("previous_marked_at")

    keep = set()
    if serving_build:
        keep.add(pvc_name(serving_build))

    now = datetime.fromisoformat(now_iso.replace("Z", "+00:00"))
    previous_aged_out = False
    if previous_build:
        previous_name = pvc_name(previous_build)
        if previous_marked_at:
            marked = datetime.fromisoformat(previous_marked_at.replace("Z", "+00:00"))
            if (now - marked) >= timedelta(hours=ttl_hours):
                previous_aged_out = True
        if not previous_aged_out:
            keep.add(previous_name)

    deleted, retained = [], []
    for name in list_index_pvcs():
        if not name.startswith(prefix):
            continue
        if name in keep:
            retained.append(name)
            continue
        delete_pvc(name)
        deleted.append(name)

    return {
        "deleted":          deleted,
        "retained":         retained,
        "previous_aged_out": [pvc_name(previous_build)] if previous_aged_out else [],
    }
