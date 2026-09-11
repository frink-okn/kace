"""Check the conversion memory derivations (src/memory_sizing.py).

Run: PYTHONPATH=src python test_memory_sizing.py

Pod memory is the ONLY memory input a webhook takes; hdtc's --memory-limit,
QLever's --stxxl-memory and the qendpoint JVM heap are all derived from it.
Both derived budgets are SOFT limits on internal buffers, not caps on RSS, so
each is a fraction of the pod rather than "whatever is left".

Grounded in two observed babel runs plus both upstream sources:
  * hdtc (ghcr.io/frink-okn/hdtc, Rust): --memory-limit 28G in a 30Gi pod was
    OOMKilled; 1G in a 28Gi pod completed. RECOMMENDED_MIN_MEMORY_BUDGET = 2GiB.
  * qlever: the merge needs numBlocks * numColumns * 500kB; babel reported 484
    blocks and failed at --stxxl-memory 1G.
"""
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT / "src"))

from config import config
from memory_sizing import (
    HDTC_WARN_SHARE,
    hdtc_budget,
    jvm_opts,
    mem_mib,
    sizing_warnings,
    stxxl_budget,
)

PODS = ("2Gi", "8Gi", "28Gi", "30Gi", "64Gi")


def main():
    config.hdtc_memory_fraction = 0.75
    config.stxxl_memory_fraction = 0.5

    for pod in PODS:
        pod_mib = mem_mib(pod)
        hdtc, stxxl, jvm = hdtc_budget(pod), stxxl_budget(pod), jvm_opts(pod)
        heap = mem_mib(jvm.split()[0].removeprefix("-Xmx"))

        # Nothing derived may sit at or near the pod limit: these are soft
        # budgets and the real footprint runs above them.
        assert mem_mib(hdtc) <= pod_mib * HDTC_WARN_SHARE, f"{pod}: hdtc {hdtc} too close"
        assert mem_mib(stxxl) <= pod_mib * HDTC_WARN_SHARE, f"{pod}: stxxl {stxxl} too close"
        assert heap <= pod_mib * HDTC_WARN_SHARE, f"{pod}: heap {heap}Mi too close"
        # Derived values must not trip the overcommit warning. A pod too small
        # for hdtc's 2GiB minimum still warns about *that* -- which is the point.
        assert not any("of pod memory" in w for w in sizing_warnings(pod, hdtc, jvm)), \
            f"{pod}: {sizing_warnings(pod, hdtc, jvm)}"

    # hdtc's own recommended minimum, and small pods that cannot honour it.
    assert mem_mib(hdtc_budget("28Gi")) >= 2048
    # A pod too small to hold the minimum with headroom: headroom wins, and the
    # shortfall is reported rather than hidden.
    assert mem_mib(hdtc_budget("2Gi")) == int(2048 * HDTC_WARN_SHARE)
    assert any("recommended minimum" in w for w in sizing_warnings("2Gi", hdtc_budget("2Gi")))

    # The run that was OOMKilled asked for far more than derivation would give.
    assert 28 * 1024 > mem_mib(hdtc_budget("30Gi"))
    # ...and the old neo4j hardcode (25G of 28Gi) would have warned.
    assert sizing_warnings("28Gi", "25G")

    # QLever's measured requirement for babel, cleared with margin.
    required_mib = 484 * 4 * 500 // 1024
    assert mem_mib(stxxl_budget("28Gi")) > required_mib * 4

    # Unparseable pod memory must not fabricate a number.
    assert hdtc_budget("lots") == "" and stxxl_budget("lots") == ""
    assert jvm_opts("lots") == "-Xss512m -XX:+UseParallelGC"
    # Non-memory JVM flags are preserved, heap is not duplicated.
    assert jvm_opts("28Gi").count("-Xmx") == 1 and "-XX:+UseParallelGC" in jvm_opts("28Gi")

    print(
        f"memory sizing checks passed (28Gi pod -> hdtc {hdtc_budget('28Gi')}, "
        f"stxxl {stxxl_budget('28Gi')}, jvm '{jvm_opts('28Gi')}', "
        f"babel needed ~{required_mib}M)"
    )


if __name__ == "__main__":
    main()
