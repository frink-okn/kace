"""Check `only_kg` subset matching for the federated index build.

Run: PYTHONPATH=src python test_kg_subset.py

A build triggered with only_kg=["dream-kg"] selected nothing, because the filter
matched the registry *shortname* ("dreamkg") while the caller used the *lakeFS
repo id* ("dream-kg"). The workflow then created a PVC, submitted a Job, and
QLever died on `!config.inputFiles_.empty()` -- an assertion about an internal
invariant, three layers away from the actual mistake.
"""
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT / "src"))

from temporal_app.activities import kg_selected


def main():
    # the pair that caused the failure: both spellings must select it
    assert kg_selected("dreamkg", "dream-kg", ["dream-kg"])
    assert kg_selected("dreamkg", "dream-kg", ["dreamkg"])

    # no filter selects everything
    assert kg_selected("dreamkg", "dream-kg", None)
    assert kg_selected("dreamkg", "dream-kg", [])

    # a filter that names something else must NOT select this KG
    assert not kg_selected("dreamkg", "dream-kg", ["geoconnex"])

    # case and stray whitespace are the caller's habits, not a reason to miss
    assert kg_selected("dreamkg", "dream-kg", ["DREAM-KG"])
    assert kg_selected("dreamkg", "dream-kg", [" dreamkg "])

    # wikidata rides the same predicate (repo == shortname there)
    assert kg_selected("wikidata", "wikidata", ["wikidata"])
    assert not kg_selected("wikidata", "wikidata", ["dream-kg"])

    # missing metadata must not crash the filter
    assert not kg_selected(None, None, ["dream-kg"])
    assert kg_selected(None, None, None)

    # and the workflow must refuse to build when a filter selects nothing
    wf = open(ROOT / "src/temporal_app/workflows/qlever_index.py").read()
    assert 'if not refs["kg_refs"]:' in wf, "workflow no longer guards the empty-source case"
    assert "inputFiles_" in wf, "the guard should name the failure it prevents"

    print("kg subset checks passed (repo id and shortname both select; empty subset refuses to build)")


if __name__ == "__main__":
    main()
