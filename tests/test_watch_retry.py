"""Check that job watchers survive a worker restart but not a real failure.

Run: PYTHONPATH=src python test_watch_retry.py

A federated index build takes ~1d17h, far longer than the interval between
routine worker restarts. The watcher heartbeats so Temporal can re-attach after
one -- but that only works if the retry policy permits it. It did not: a
`rollout restart` at 21:44 killed build 20260825-204430 five minutes later,
exactly heartbeat_timeout, with retry_policy=NO_RETRY.

So: watching must be retryable, while terminal outcomes (Job failed, Job gone)
must not be -- otherwise a genuine failure is re-read 20 times before surfacing.
"""
import ast
import sys
from pathlib import Path

from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT / "src"))

WORKFLOWS = (ROOT / "src/temporal_app/workflows")


def watcher_policies():
    """(file, policy) for every watch_k8s_job_sync call in the workflows."""
    out = []
    for path in sorted(WORKFLOWS.glob("*.py")):
        tree = ast.parse(path.read_text())
        for node in ast.walk(tree):
            if not isinstance(node, ast.Call):
                continue
            args = [a for a in node.args if isinstance(a, ast.Name)]
            if not any(a.id == "watch_k8s_job_sync" for a in args):
                continue
            kw = {k.arg: k.value for k in node.keywords}
            policy = kw.get("retry_policy")
            out.append((path.name, getattr(policy, "id", None), "heartbeat_timeout" in kw))
    return out


def main():
    watchers = watcher_policies()
    assert watchers, "no watch_k8s_job_sync call sites found -- did the wiring change?"

    for fname, policy, has_heartbeat in watchers:
        assert policy != "NO_RETRY", (
            f"{fname}: a watcher with NO_RETRY cannot survive a worker restart"
        )
        assert policy == "WATCH_RETRY", f"{fname}: unexpected retry policy {policy}"
        assert has_heartbeat, f"{fname}: a retryable watcher still needs heartbeat_timeout"

    # The policy itself must allow more than one attempt.
    for path in sorted(WORKFLOWS.glob("*.py")):
        src = path.read_text()
        if "WATCH_RETRY = RetryPolicy(" not in src:
            continue
        tree = ast.parse(src)
        for node in ast.walk(tree):
            if isinstance(node, ast.Assign) and getattr(node.targets[0], "id", "") == "WATCH_RETRY":
                kw = {k.arg: k.value for k in node.value.keywords}
                attempts = getattr(kw.get("maximum_attempts"), "value", 1)
                assert attempts > 1, f"{path.name}: WATCH_RETRY allows only {attempts} attempt"

    # Terminal outcomes must be raised non-retryable, or a failed Job would be
    # re-read for every one of those attempts.
    acts = (ROOT / "src/temporal_app/activities.py").read_text()
    watch_src = acts[acts.index("async def watch_k8s_job_sync"):]
    watch_src = watch_src[:watch_src.index("\n@activity.defn")]
    assert watch_src.count("non_retryable=True") == 2, (
        "both terminal paths (job failed, job missing) must be non-retryable"
    )
    assert "raise Exception(" not in watch_src, "a bare Exception here would be retried"

    print(f"watch retry checks passed ({len(watchers)} watchers retryable, terminal failures are not)")


if __name__ == "__main__":
    main()
