"""Sizing the memory flags the conversion jobs take.

Both flags this module derives are SOFT budgets for internal buffers, not caps
on RSS, so neither can sit at parity with the pod's memory limit:

  * hdtc's `--memory-limit` (ghcr.io/frink-okn/hdtc, Rust) is a budget its
    pipeline divides across stages. Observed: 28G inside a 30Gi pod (93%) was
    OOMKilled, while 1G inside a 28Gi pod completed. hdtc's own
    RECOMMENDED_MIN_MEMORY_BUDGET is 2GiB.

  * QLever's `--stxxl-memory` bounds the external sorter. Its real requirement
    is `numBlocks * numColumns * 500kB` (DEFAULT_BLOCKSIZE_EXTERNAL_ID_TABLE in
    CompressedExternalIdTable.h) -- a property of the DATA, not the pod, and not
    knowable before a build runs. babel reported 484 blocks (~945MiB) and failed
    at 1G. A fraction of the pod is therefore a proxy that holds only because
    pods are already sized roughly by dataset; when a build reports
    "Insufficient memory for merging N blocks", the floor is N * 4 * 500kB.

The JVM jobs (qendpoint: nt-merge, riot, void) run in their OWN pods, so a heap
size from java_opts never competes with hdtc for the same cgroup.

Both coefficients are config so they can be retuned without a rebuild.
"""
import re

from config import config

# hdtc refuses to plan well below this; see RECOMMENDED_MIN_MEMORY_BUDGET.
HDTC_MIN_BUDGET_MIB = 2048
# Above this share of the pod, hdtc's real footprint is likely to exceed the
# limit. 93% was fatal in practice.
HDTC_WARN_SHARE = 0.85
# Non-memory JVM flags for the qendpoint jobs; -Xmx/-Xms are derived per pod.
JVM_DEFAULT_FLAGS = "-Xss512m -XX:+UseParallelGC"


def mem_mib(value) -> int | None:
    """Parse '28Gi' / '28G' / '4096M' / '512K' to MiB. None if unparseable."""
    if not value:
        return None
    m = re.fullmatch(r"(\d+(?:\.\d+)?)\s*(Gi?|Mi?|Ki?|Ti?)?", str(value).strip(), re.IGNORECASE)
    if not m:
        return None
    unit = (m.group(2) or "M").upper().rstrip("I")
    return int(float(m.group(1)) * {"K": 1 / 1024, "M": 1, "G": 1024, "T": 1024 * 1024}[unit])


def hdtc_budget(pod_memory: str) -> str:
    """hdtc --memory-limit for a pod of this size.

    The ceiling wins over the floor: a pod too small to give hdtc its 2GiB
    recommended minimum *and* keep headroom gets the headroom, because a soft
    budget at 100% of the pod is an OOMKill rather than a slow build. That case
    reports through sizing_warnings() instead of being silently papered over.
    """
    pod_mib = mem_mib(pod_memory)
    if pod_mib is None:
        return ""
    floor = min(HDTC_MIN_BUDGET_MIB, pod_mib)
    ceiling = int(pod_mib * HDTC_WARN_SHARE)
    return f"{min(max(int(pod_mib * config.hdtc_memory_fraction), floor), ceiling)}M"


def stxxl_budget(pod_memory: str) -> str:
    """QLever --stxxl-memory for a pod of this size."""
    pod_mib = mem_mib(pod_memory)
    if pod_mib is None:
        return ""
    return f"{int(pod_mib * config.stxxl_memory_fraction)}M"


def jvm_opts(pod_memory: str, extra_flags: str = JVM_DEFAULT_FLAGS) -> str:
    """JAVA_OPTIONS for the qendpoint jobs, heap sized from the same pod.

    Those jobs run in their own pods, so the heap competes with nothing but the
    JVM's own non-heap footprint -- hence the same fraction as hdtc rather than
    a separate knob. Callers keep control of the non-memory flags (GC, stack
    size); only -Xmx/-Xms are derived.
    """
    pod_mib = mem_mib(pod_memory)
    if pod_mib is None:
        return extra_flags
    heap = max(int(pod_mib * config.hdtc_memory_fraction), min(1024, pod_mib))
    return f"-Xmx{heap}m -Xms{heap}m {extra_flags}".strip()


def sizing_warnings(pod_memory: str, program_memory: str, java_opts: str = "") -> list[str]:
    """Human-readable warnings about a memory combination.

    Everything is derived now, so these should never fire in normal operation --
    they exist to catch a bad coefficient in config, or a workflow started
    directly with hand-picked values.
    """
    out = []
    pod_mib, prog_mib = mem_mib(pod_memory), mem_mib(program_memory)
    if prog_mib and pod_mib and prog_mib > pod_mib * HDTC_WARN_SHARE:
        out.append(
            f"mem_size={program_memory} is {prog_mib / pod_mib:.0%} of pod memory="
            f"{pod_memory}; it is a soft limit, hdtc's real footprint sits above "
            f"it, and 93% was OOMKilled in practice."
        )
    if prog_mib and prog_mib < HDTC_MIN_BUDGET_MIB:
        out.append(
            f"mem_size={program_memory} is below hdtc's recommended minimum budget "
            f"of {HDTC_MIN_BUDGET_MIB}MiB; the build will spill more than it needs to."
        )
    heap = re.search(r"-Xmx(\S+)", java_opts or "")
    heap_mib = mem_mib(heap.group(1)) if heap else None
    if heap_mib and pod_mib and heap_mib + HDTC_MIN_BUDGET_MIB > pod_mib:
        out.append(
            f"JVM heap {heap_mib}Mi (java_opts) leaves under 2Gi of headroom in a "
            f"{pod_memory} pod; the qendpoint jobs may be OOMKilled."
        )
    return out
