"""Check per-repo skip in the federated qlever index download manifest.

Run: PYTHONPATH=src python test_qlever_change_detection.py

Whole-build change detection (qlever_index.py phase 2) only skips the ENTIRE
build when nothing anywhere changed. Any real run has at least one KG with a
new commit, so prepare_qlever_job_specs used to re-list every repo's file for
download regardless of whether that repo changed -- on a cross-cluster setup
(kace local, lakeFS remote) that is a full egress bill every single build.
"""
import asyncio
import os
import sys
import tempfile
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT / "src"))

TMP = tempfile.mkdtemp()
os.environ.setdefault("QLEVER_SOURCE_PATH", TMP)
os.environ.setdefault("K8S_NAMESPACE", "test")

from temporal_app.activities import prepare_qlever_job_specs  # noqa: E402


def _touch(path, content=b"x"):
    os.makedirs(os.path.dirname(path), exist_ok=True)
    with open(path, "wb") as f:
        f.write(content)


def main():
    source_root = TMP

    # unchanged-repo: commit matches state, file present+non-empty -> skip.
    _touch(f"{source_root}/kg-unchanged/graph.nt.gz")
    # changed-repo: commit moved since state -> must still be downloaded.
    _touch(f"{source_root}/kg-changed/graph.nt.gz")
    # missing-file: commit matches state but the PVC lost the file -> must
    # still be downloaded (a commit match alone is not enough).
    # (no file written for kg-missing-file)

    kg_refs = {
        "kg-unchanged": {"shortname": "unchanged", "ref": "v1.0.0", "commit": "aaa", "remote_path": "nt/graph.nt.gz"},
        "kg-changed": {"shortname": "changed", "ref": "v2.0.0", "commit": "new-commit", "remote_path": "nt/graph.nt.gz"},
        "kg-missing-file": {"shortname": "missingfile", "ref": "v1.0.0", "commit": "bbb", "remote_path": "nt/graph.nt.gz"},
        "okn-void": {"shortname": "void", "ref": "stable_v1.0.0", "commit": "void-commit", "remote_path": "void/void.nt"},
    }
    state = {
        "source_commits": {
            "kg-unchanged": "aaa",
            "kg-changed": "old-commit",
            "kg-missing-file": "bbb",
            # okn-void deliberately absent -- excluded from whole-build change
            # detection, so it has no stored commit and must always download.
        }
    }

    specs = asyncio.run(prepare_qlever_job_specs(kg_refs, "v0.0.1", None, state, "s2-commit"))
    downloaded_repos = {d["repo"] for d in specs["downloads"]}

    assert "kg-unchanged" not in downloaded_repos, "unchanged repo with an intact local file must be skipped"
    assert "kg-changed" in downloaded_repos, "a repo with a new commit must still be downloaded"
    assert "kg-missing-file" in downloaded_repos, "a matching commit does not excuse a missing local file"
    assert "okn-void" in downloaded_repos, "void has no stored commit (excluded from change detection) -- always downloads"

    # build_command must still reference every repo regardless of skip -- the
    # indexer needs all of them, skip only means "already on the PVC".
    assert "kg-unchanged" in specs["build_command"] or "unchanged" in specs["build_command"]

    # no state at all (first run, e.g. a fresh cluster) -> nothing to compare
    # against, everything downloads.
    specs_no_state = asyncio.run(prepare_qlever_job_specs(kg_refs, "v0.0.1", None, None, "s2-commit"))
    assert set(kg_refs.keys()) <= {d["repo"] for d in specs_no_state["downloads"]}

    print("qlever change-detection checks passed (unchanged+intact repos skip; changed/missing/void always download)")


if __name__ == "__main__":
    main()
