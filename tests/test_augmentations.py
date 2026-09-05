"""Check the augmentation commands (src/temporal_app/augmentations.py).

Run: PYTHONPATH=src python3 tests/test_augmentations.py

Three production failures on scales-kg:

  * `hdtc create --index` writes the index next to the output with the output's
    extension REPLACED by `hdt` — output `graph.hdt.merged` produced
    `graph.hdt.hdt.index.v1-1`, so `mv graph.hdt.merged.index.v1-1 ...` died
    with "No such file or directory" after 20 minutes of merging, and the
    workflow retried the same 20 minutes forever.
  * A step whose `from` predicate matches nothing (the registry had an IRI that
    does not occur in the KG) leaves a 20-byte empty aug file, and the merge
    then spent those 20 minutes folding in zero triples.

  * `hdtc search` prints TAB-delimited N-Triples, but the map-predicate step
    swapped the predicate with a sed pattern written for spaces. It matched
    nothing, exited 0, and emitted the ORIGINAL triples — which the merge folds
    back in as duplicates. A silent no-op indistinguishable from success.

The bash is exercised for real against a stub `hdtc` so the guard, the mv
targets and the predicate swap are proved, not just pattern-matched.
"""
import os
import subprocess
import sys
import gzip
import tempfile
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT / "src"))

from temporal_app.augmentations import build_augmentation_jobs, build_merge_command


def hdtc_index_path(output: str) -> str:
    """How `hdtc create --index` names its index file: the documented
    `.hdt.index.v1-1` suffix is the output path with its extension replaced by
    `hdt`, then `.index.v1-1` appended."""
    return str(Path(output).with_suffix(".hdt")) + ".index.v1-1"


def run(cmd: str, cwd: Path) -> subprocess.CompletedProcess:
    return subprocess.run(["/bin/bash", "-c", cmd], cwd=cwd,
                          capture_output=True, text=True,
                          env={**os.environ, "PATH": f"{cwd}/bin:{os.environ['PATH']}"})


def stub_hdtc(root: Path) -> None:
    """A `hdtc` that writes the output and the index where the real one does."""
    (root / "bin").mkdir(parents=True, exist_ok=True)
    stub = root / "bin" / "hdtc"
    stub.write_text(
        '#!/bin/bash\n'
        'out=""; while [ $# -gt 0 ]; do case "$1" in --output) out="$2"; shift;; esac; shift; done\n'
        'echo merged > "$out"\n'
        'echo index > "${out%.*}.hdt.index.v1-1"\n'
    )
    stub.chmod(0o755)


def scenario(aug_bytes: bytes):
    """A working dir with graph.hdt + its index, nt/graph.nt.gz and one aug
    file holding `aug_bytes` (uncompressed), plus the merge command for it."""
    root = Path(tempfile.mkdtemp())
    stub_hdtc(root)
    for sub in ("hdt", "nt", "aug", "hdt-tmp"):
        (root / sub).mkdir()
    hdt = root / "hdt" / "graph.hdt"
    hdt.write_text("original")
    (root / "hdt" / "graph.hdt.index.v1-1").write_text("original index")
    nt = root / "nt" / "graph.nt.gz"
    nt.write_bytes(gzip.compress(b"<s> <p> <o> .\n"))
    aug = root / "aug" / "00-map-predicate.nt.gz"
    aug.write_bytes(gzip.compress(aug_bytes))
    cmd = build_merge_command(hdt_file=str(hdt), nt_file=str(nt),
                              aug_files=[str(aug)],
                              temp_dir=str(root / "hdt-tmp"), memory_limit="4G")
    return root, hdt, nt, cmd


class Step:
    def __init__(self, name, params):
        self.name, self.params = name, params


def stub_hdtc_search(root: Path, triples: list) -> None:
    """A `hdtc` whose `search` prints what the real one prints: TAB-delimited
    N-Triples, `S\tP\tO\t.`, with its own logging on stderr."""
    (root / "bin").mkdir(parents=True, exist_ok=True)
    body = "\n".join("\t".join(t) + "\t." for t in triples)
    stub = root / "bin" / "hdtc"
    stub.write_text(
        "#!/bin/bash\n"
        'echo "INFO hdtc - HDT Creator" >&2\n'
        f"cat <<'EOT'\n{body}\nEOT\n"
    )
    stub.chmod(0o755)


# 0. map-predicate rewrites the predicate of TAB-delimited hdtc output, and
#    leaves subject and object — including a literal carrying an escaped tab —
#    exactly as they were.
FROM_P = "http://release.niem.gov/niem/niem-core/5.0/#PersonFullName"
TO_P = "http://www.w3.org/2000/01/rdf-schema#label"
root = Path(tempfile.mkdtemp())
(root / "aug").mkdir()
stub_hdtc_search(root, [
    ("<http://ex.org/s1>", f"<{FROM_P}>", '"Timothy M. Burgess"'),
    ("<http://ex.org/s2>", f"<{FROM_P}>", r'"Office\tAnnex"'),
])
jobs = build_augmentation_jobs([Step("map-predicate", {"from": FROM_P, "to": TO_P})],
                               str(root / "graph.hdt"), str(root / "aug"))
suffix, out_file, cmd = jobs[0]
assert suffix == "aug0" and out_file.endswith("00-map-predicate.nt.gz")
res = run(cmd, root)
assert res.returncode == 0, res.stderr
lines = gzip.decompress(Path(out_file).read_bytes()).decode().splitlines()
assert lines == [
    f"<http://ex.org/s1>\t<{TO_P}>\t\"Timothy M. Burgess\"\t.",
    f"<http://ex.org/s2>\t<{TO_P}>\t\"Office\\tAnnex\"\t.",
], lines
assert FROM_P not in "\n".join(lines), "source predicate survived the swap"

# 1. The mv target agrees with how hdtc actually names the index.
_, hdt, _, cmd = scenario(b"<a> <b> <c> .\n")
merged = f"{hdt}.merged.hdt"
assert f"mv {hdtc_index_path(merged)} {hdt}.index.v1-1" in cmd, cmd
assert f"--output {merged}" in cmd, cmd

# 2. Non-empty aug: merge runs, both artifacts are replaced, nt is appended.
root, hdt, nt, cmd = scenario(b"<a> <b> <c> .\n")
res = run(cmd, root)
assert res.returncode == 0, res.stderr
assert hdt.read_text() == "merged\n", hdt.read_text()
assert Path(f"{hdt}.index.v1-1").read_text() == "index\n"
assert not Path(merged).exists() and not Path(f"{merged}.index.v1-1").exists()
assert gzip.decompress(nt.read_bytes()) == b"<s> <p> <o> .\n<a> <b> <c> .\n"

# 3. Empty aug: exit 0 without touching hdtc or any artifact.
root, hdt, nt, cmd = scenario(b"")
res = run(cmd, root)
assert res.returncode == 0, res.stderr
assert "skipping merge" in res.stdout, res.stdout
assert hdt.read_text() == "original"
assert Path(f"{hdt}.index.v1-1").read_text() == "original index"
assert gzip.decompress(nt.read_bytes()) == b"<s> <p> <o> .\n"

# 4. The emptiness guard survives a large aug file (gzip takes SIGPIPE from
#    `head -c1` under `set -o pipefail`; that status must not read as empty).
root, hdt, nt, cmd = scenario(b"<a> <b> <c> .\n" * 200000)
res = run(cmd, root)
assert res.returncode == 0, res.stderr
assert "skipping merge" not in res.stdout, "large aug file wrongly treated as empty"
assert hdt.read_text() == "merged\n"

print("augmentations: predicate swap survives hdtc's tab-delimited output; "
      "index mv matches .hdt.index.v1-1 naming; empty aug skips the merge")
