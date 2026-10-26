"""Check that everything the worker registers is actually registerable.

Run: PYTHONPATH=src python tests/test_worker_registration.py

The worker validates its activity list at construction, so a function that
lost its @activity.defn does not fail at import or in a unit check -- it fails
when the container starts, as a CrashLoopBackOff with

    TypeError: Activity <name> missing attributes, was it decorated with
    @activity.defn?

which is a slow and expensive way to learn it. That happened for real: a helper
inserted directly above `async def resolve_qlever_refs` landed BETWEEN the
decorator and the function, silently moving @activity.defn onto the helper.
"""
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT / "src"))

from temporalio.activity import _Definition  # noqa: E402


def main():
    import temporal_app.worker as worker

    src = (ROOT / "src/temporal_app/worker.py").read_text()
    listed = src[src.index("activities=["):]
    listed = listed[:listed.index("]")]
    names = []
    for line in listed.splitlines()[1:]:
        name = line.split("#")[0].strip().rstrip(",").strip()   # comment first, then comma
        if name:
            names.append(name)
    assert len(names) > 30, f"only found {len(names)} registered activities -- parse broke?"

    undecorated = []
    for name in names:
        fn = getattr(worker, name, None)
        assert fn is not None, f"worker.py registers {name} but does not import it"
        try:
            _Definition.must_from_callable(fn)   # exactly what Worker() does
        except TypeError as e:
            undecorated.append(f"{name}: {e}")
    assert not undecorated, "activities the Worker would reject:\n  " + "\n  ".join(undecorated)

    # and the workflows it registers must be real workflow definitions
    for wf in ("HDTConversionWorkflow", "QLeverIndexWorkflow", "QLeverDeploymentWorkflow"):
        cls = getattr(worker, wf)
        assert getattr(cls, "__temporal_workflow_definition", None), f"{wf} lost @workflow.defn"

    print(f"worker registration checks passed ({len(names)} activities all decorated)")


if __name__ == "__main__":
    main()
