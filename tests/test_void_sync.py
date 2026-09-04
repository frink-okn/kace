"""Check the okn-void release cycle driven by the weekly federated build.

Run: PYTHONPATH=src python tests/test_void_sync.py

Three ways this can silently serve the wrong voids:

  * branch ordering by string -- 'stable_v0_0_9' > 'stable_v0_0_15'
    lexicographically, so a naive max() tags last week's build forever.
  * tagging a stable branch that has not been built yet. The branch is cut
    from main, which still carries the *previous* release's void/void.nt, so
    file presence proves nothing -- okn-void's pre-create-tag lua hook checks
    the pav:version triple, and if we skip that check the tag is rejected
    (or worse, deploys an endpoint built for another version).
  * the tag being created by the index build instead of the federation
    rollover, which would flip the void endpoint days before the federated
    index that matches it.
"""
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT / "src"))

from temporal_app.activities import (
    newest_void_stable,
    void_build_ready,
    void_stable_version,
)


def main():
    # branch name -> version, and nothing else parses
    assert void_stable_version("stable_v0_0_15") == "v0.0.15"
    assert void_stable_version("stable_v1_2_3") == "v1.2.3"
    assert void_stable_version("main") == ""
    assert void_stable_version("develop") == ""
    assert void_stable_version("stable_v0_0_15_backup") == ""
    assert void_stable_version(None) == ""

    # the ordering bug: numeric, not lexicographic
    branches = ["main", "develop", "stable_v0_0_9", "stable_v0_0_15", "stable_v0_0_14"]
    assert newest_void_stable(branches) == ("stable_v0_0_15", "v0.0.15")
    assert newest_void_stable(["main"]) == ("", "")
    assert newest_void_stable([]) == ("", "")

    # readiness: only the matching pav:version counts as built
    fresh = 'IRI> <http://purl.org/pav/version> "v0.0.15" .\n'
    stale = 'IRI> <http://purl.org/pav/version> "v0.0.14" .\n'
    assert void_build_ready(fresh, "v0.0.15")
    assert not void_build_ready(stale, "v0.0.15")      # branch cut from main
    assert not void_build_ready("", "v0.0.15")
    assert not void_build_ready(None, "v0.0.15")

    # the tag is the rollout trigger, so it belongs to the rollover, not the build
    index_wf = (ROOT / "src/temporal_app/workflows/qlever_index.py").read_text()
    deploy_wf = (ROOT / "src/temporal_app/workflows/qlever_federation_deployment.py").read_text()
    assert "sync_void_repo" in index_wf and "wait_void_artifacts" in index_wf
    assert "tag_void_build" not in index_wf
    assert "tag_void_build" in deploy_wf
    # ...and only on a serving rollover; a rollback must not advance the release
    assert deploy_wf.index('resolved["source"] == "serving"') < deploy_wf.index("tag_void_build,\n                    start_to_close")

    # a void hiccup must never fail the multi-day index build
    assert "_sync_void" in index_wf and "except Exception" in index_wf

    # every new activity is registered, or Temporal rejects it at runtime
    worker = (ROOT / "src/temporal_app/worker.py").read_text()
    for act in ("sync_void_repo", "wait_void_artifacts", "tag_void_build"):
        assert worker.count(act) == 2, act

    print("ok: void stable branch ordering, build-readiness, and tag placement pinned")


if __name__ == "__main__":
    main()
