"""K8s ConfigMap-backed state for the federated QLever index build.

Stores the currently-serving and previous build IDs (and the source commits
that produced the serving build) so that QLeverIndexWorkflow can:
  * skip rebuilds when no source commit has changed
  * roll over to a new per-build PVC and GC the prior one after a TTL
  * decide which output PVCs are orphans during phase-0 GC
"""
import json
from typing import Dict, Optional
from kubernetes import client, config as k8s_config
from kubernetes.client.rest import ApiException
from config import config as app_config


def _reload_k8s_auth():
    # legacy no-op; kept for source-compat
    pass


def _fresh_api_client():
    cfg = client.Configuration()
    k8s_config.load_incluster_config(client_configuration=cfg)
    return client.ApiClient(configuration=cfg)


EMPTY_STATE: Dict = {
    "build_id_serving": None,
    "build_id_previous": None,
    "previous_marked_at": None,
    "source_commits": {},
    "image": None,
}


def _api() -> client.CoreV1Api:
    return client.CoreV1Api(api_client=_fresh_api_client())


def read_state() -> Dict:
    name = app_config.qlever_state_configmap
    namespace = app_config.k8s_namespace
    try:
        cm = _api().read_namespaced_config_map(name=name, namespace=namespace)
    except ApiException as e:
        if e.status == 404:
            return dict(EMPTY_STATE, source_commits={})
        raise
    data = cm.data or {}
    return {
        "build_id_serving":   data.get("build_id_serving")  or None,
        "build_id_previous":  data.get("build_id_previous") or None,
        "previous_marked_at": data.get("previous_marked_at") or None,
        "source_commits":     json.loads(data.get("source_commits.json", "{}")),
        "image":              data.get("image") or None,
    }


def write_state(state: Dict) -> None:
    name = app_config.qlever_state_configmap
    namespace = app_config.k8s_namespace
    body = {
        "apiVersion": "v1",
        "kind": "ConfigMap",
        "metadata": {"name": name, "namespace": namespace},
        "data": {
            "build_id_serving":     state.get("build_id_serving") or "",
            "build_id_previous":    state.get("build_id_previous") or "",
            "previous_marked_at":   state.get("previous_marked_at") or "",
            "image":                state.get("image") or "",
            "source_commits.json":  json.dumps(state.get("source_commits", {}), indent=2, sort_keys=True),
        },
    }
    api = _api()
    try:
        api.read_namespaced_config_map(name=name, namespace=namespace)
        api.patch_namespaced_config_map(name=name, namespace=namespace, body=body)
    except ApiException as e:
        if e.status == 404:
            api.create_namespaced_config_map(namespace=namespace, body=body)
            return
        raise
