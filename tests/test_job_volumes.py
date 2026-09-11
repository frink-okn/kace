"""Check that a Job never declares one PVC through two volumes.

Run: PYTHONPATH=src python test_job_volumes.py

A pod that names the same claim in two volumes wedges kubelet on this cluster's
CSI driver: the sandbox never completes, the pod sits in ContainerCreating
indefinitely, and NO event says why. Reproduced with a two-volume/one-claim
busybox pod, which hung while a one-volume version started in 15s.

KACE hits this whenever local_pvc_name == shared_pvc_name (a single-volume
install): the default /mnt/repo mount and the /shared extra_pvc name one PVC.
"""
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT / "src"))

from kubernetes import client  # noqa: E402


def build_pod_template(default_claim):
    """The shape JobMan.init_job_object produces: one default `data` volume."""
    return client.V1PodSpec(
        containers=[client.V1Container(name="job", image="x")],
        volumes=[
            client.V1Volume(
                name="data",
                persistent_volume_claim=client.V1PersistentVolumeClaimVolumeSource(
                    claim_name=default_claim
                ),
            )
        ],
    )


def apply(default_claim, extra_pvcs):
    """Run the real assembly used by JobMan.run_job -- no cluster needed."""
    from k8s.podman import attach_extra_pvcs

    pod_template = build_pod_template(default_claim)
    volume_mounts = [client.V1VolumeMount(name="data", mount_path="/mnt/repo", read_only=True)]
    return attach_extra_pvcs(pod_template, extra_pvcs, volume_mounts)


def claims_of(pod_template):
    return [v.persistent_volume_claim.claim_name for v in pod_template.volumes if v.persistent_volume_claim]


def main():
    # 1. single-volume install: /mnt/repo and /shared are the SAME claim
    pt, mounts = apply("frink-ldf-storage-ssd", [
        {"name": "shared-source", "claim": "frink-ldf-storage-ssd", "mount_path": "/shared", "read_only": True},
        {"name": "index", "claim": "kace-qlever-index-1", "mount_path": "/index"},
    ])
    claims = claims_of(pt)
    assert len(claims) == len(set(claims)), f"one claim declared twice: {claims}"
    # both paths still get mounted, through one volume
    paths = {m.mount_path: m.name for m in mounts}
    assert paths["/mnt/repo"] == paths["/shared"] == "data", paths
    assert paths["/index"] == "index"

    # 2. two-volume install (GKE): distinct claims keep distinct volumes
    pt2, mounts2 = apply("frink-kace-ssd", [
        {"name": "shared-source", "claim": "frink-ldf-storage-ssd", "mount_path": "/shared", "read_only": True},
        {"name": "index", "claim": "kace-qlever-index-1", "mount_path": "/index"},
    ])
    assert claims_of(pt2) == ["frink-kace-ssd", "frink-ldf-storage-ssd", "kace-qlever-index-1"]
    assert {m.mount_path for m in mounts2} == {"/mnt/repo", "/shared", "/index"}

    # 3. run_job must actually route through this helper
    src = open(ROOT / "src/k8s/podman.py").read()
    assert "attach_extra_pvcs(pod_template, extra_pvcs, volume_mounts)" in src, \
        "run_job no longer uses attach_extra_pvcs"

    print("job volume checks passed (one volume per claim; every mount path preserved)")


if __name__ == "__main__":
    main()
