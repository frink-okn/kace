"""Kubernetes API clients for the two clusters KACE spans.

KACE runs its batch work (conversions, index builds) on the cluster it lives in
and manages the serving layer (per-KG QLever, LDF, federated QLever) on a second,
remote cluster. Every k8s call therefore has to answer one question first: which
cluster? That answer is a `cluster` argument threaded from the call site --
`LOCAL` for anything that shares a PVC with the worker process, `REMOTE` for
anything a user's browser eventually talks to.

Single-cluster mode is the default and costs nothing: with `REMOTE_KUBECONFIG`
unset, `effective()` folds REMOTE into LOCAL and every call takes the in-cluster
path exactly as it did before the split existed.
"""
import os
from kubernetes import client, config as k8s_config

from config import config as app_config
from log_util import LoggingUtil

logger = LoggingUtil.init_logging(__name__)

LOCAL = "local"
REMOTE = "remote"

_TOKEN_FILE = "/var/run/secrets/kubernetes.io/serviceaccount/token"

# Remote clients are cached on the kubeconfig's (path, mtime). Two reasons:
# the job watcher polls every 5s and a fresh TLS handshake per poll over a WAN
# is real latency, and mtime-keying still picks up a rotated credential without
# needing a worker restart.
_remote_cache = {}


def remote_configured() -> bool:
    return bool(app_config.remote_kubeconfig) and os.path.exists(app_config.remote_kubeconfig)


def effective(cluster: str) -> str:
    """Resolve a requested cluster against what is actually configured.

    Call sites declare intent (REMOTE = "this belongs with the serving layer");
    this decides whether that intent has anywhere to go. Always gate behavior on
    the result, never on the raw argument -- otherwise single-cluster mode starts
    taking remote-only code paths.
    """
    if cluster == REMOTE and remote_configured():
        return REMOTE
    return LOCAL


def namespace(cluster: str = LOCAL) -> str:
    if effective(cluster) == REMOTE and app_config.remote_namespace:
        return app_config.remote_namespace
    return app_config.k8s_namespace


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


def _local_api_client():
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
        k8s_config.load_incluster_config(client_configuration=cfg)
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
        logger.warning("SA token file is empty; falling back to client default auth.")
    except Exception as e:
        logger.warning(f"Manual token attach failed, falling back to client default: {e}")

    return client.ApiClient(configuration=cfg)


def _remote_api_client():
    """Client for the serving cluster, from a static kubeconfig.

    The kubeconfig must be self-contained: server URL, CA data, and a bearer
    token. A `gcloud container clusters get-credentials` kubeconfig will NOT
    work -- it shells out to `gke-gcloud-auth-plugin`, which is not in this
    image. See deploy/gke-remote-rbac.yaml for how to mint one.
    """
    path = app_config.remote_kubeconfig
    key = (path, os.path.getmtime(path))
    cached = _remote_cache.get(key)
    if cached is not None:
        return cached

    cfg = client.Configuration()
    k8s_config.load_kube_config(
        config_file=path,
        context=app_config.remote_kube_context or None,
        client_configuration=cfg,
    )
    api_client = client.ApiClient(configuration=cfg)
    _remote_cache.clear()  # only ever one live remote config; drop stale mtimes
    _remote_cache[key] = api_client
    logger.info(f"Built remote k8s client from {path} "
                f"(context={app_config.remote_kube_context or 'current-context'})")
    return api_client


def api_client(cluster: str = LOCAL):
    if effective(cluster) == REMOTE:
        return _remote_api_client()
    return _local_api_client()


def core_v1(cluster: str = LOCAL) -> client.CoreV1Api:
    return client.CoreV1Api(api_client=api_client(cluster))


def apps_v1(cluster: str = LOCAL) -> client.AppsV1Api:
    return client.AppsV1Api(api_client=api_client(cluster))


def batch_v1(cluster: str = LOCAL) -> client.BatchV1Api:
    return client.BatchV1Api(api_client=api_client(cluster))


def custom_objects(cluster: str = LOCAL) -> client.CustomObjectsApi:
    return client.CustomObjectsApi(api_client=api_client(cluster))


def networking_v1(cluster: str = LOCAL) -> client.NetworkingV1Api:
    return client.NetworkingV1Api(api_client=api_client(cluster))
