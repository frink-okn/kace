"""Check cluster selection in k8s/clients.py.

Run: PYTHONPATH=src python test_clients.py

The logic under test is small but load-bearing: get it wrong and serving
objects get created on the build cluster (or vice versa), which no unit of the
rest of the system would notice.
"""
import os
import tempfile

from config import config
from k8s import clients

KUBECONFIG = """\
apiVersion: v1
kind: Config
clusters:
- name: serving
  cluster:
    server: https://serving.example:443
    insecure-skip-tls-verify: true
contexts:
- name: serving
  context: {cluster: serving, user: kace-remote, namespace: frink-serving}
current-context: serving
users:
- name: kace-remote
  user: {token: not-a-real-token}
"""


def main():
    config.k8s_namespace = "frink-build"

    # --- no kubeconfig: single-cluster mode, REMOTE folds into LOCAL --------
    config.remote_kubeconfig = ""
    config.remote_namespace = ""
    assert clients.effective(clients.REMOTE) == clients.LOCAL
    assert clients.namespace(clients.REMOTE) == "frink-build"

    # A configured-but-missing path must NOT silently become remote.
    config.remote_kubeconfig = "/nonexistent/kubeconfig"
    assert clients.effective(clients.REMOTE) == clients.LOCAL

    # --- with a kubeconfig: remote resolves, local stays local --------------
    with tempfile.NamedTemporaryFile("w", suffix=".kubeconfig", delete=False) as fh:
        fh.write(KUBECONFIG)
        path = fh.name
    try:
        config.remote_kubeconfig = path
        assert clients.effective(clients.REMOTE) == clients.REMOTE
        assert clients.effective(clients.LOCAL) == clients.LOCAL

        # namespace falls back to the shared name until overridden
        assert clients.namespace(clients.REMOTE) == "frink-build"
        config.remote_namespace = "frink-serving"
        assert clients.namespace(clients.REMOTE) == "frink-serving"
        assert clients.namespace(clients.LOCAL) == "frink-build"

        api = clients.core_v1(clients.REMOTE)
        host = api.api_client.configuration.host
        assert host == "https://serving.example:443", host
        # cached per (path, mtime) so the 5s job poll isn't a TLS handshake each time
        assert clients.core_v1(clients.REMOTE).api_client is api.api_client
    finally:
        os.unlink(path)

    print("clients checks passed")


if __name__ == "__main__":
    main()
