"""Registry-configurable augmentation steps for the HDT conversion pipeline.

Augmentations are additive-only by construction: each step reads the
already-built (and indexed) HDT and writes brand-new triples to its own
gzipped N-Triples file under {working_dir}/aug/; a final merge job feeds the
original HDT plus every aug file back through `hdtc create` and appends the
aug triples to nt/graph.nt.gz. No step can remove or modify existing triples
— destructive transformations are deliberately unsupported.

Steps are configured per-KG in the okn-registry kgs.yaml:

    frink-options:
      lakefs-repo: scales
      augmentations:
        - name: map-predicate
          params:
            from: https://scales.okn.us/property/hasName   # str or list
            to: http://www.w3.org/2000/01/rdf-schema#label

To add a new augmentation type: write a builder(params, hdt_file, out_file)
that raises ValueError on bad params and returns a bash command writing
gzipped N-Triples to out_file, then register it in AUGMENTATION_CATALOG.
Commands run in the hdtc image (debian bookworm-slim; bash/sed/gzip present)
with the repo working dir mounted — same setup as the nt-merge job.

Everything here is pure string-building (no I/O), so workflows may call it
directly without breaking replay determinism.
"""
import re

# Absolute IRI containing none of the characters that would break the
# quoting in the shell/sed pipelines below (whitespace, angle brackets,
# quotes, backtick, pipe, backslash).
_SAFE_IRI = re.compile(r"^[A-Za-z][A-Za-z0-9+.\-]*:[^\s<>\"'`|\\]+$")


def _iri(value, step_name: str, param: str) -> str:
    if not isinstance(value, str) or not _SAFE_IRI.match(value):
        raise ValueError(
            f"augmentation '{step_name}': param '{param}' must be an absolute IRI "
            f"without whitespace, quotes, angle brackets or pipes — got {value!r}"
        )
    return value


def _iri_list(value, step_name: str, param: str) -> list:
    values = value if isinstance(value, list) else [value]
    if not values:
        raise ValueError(f"augmentation '{step_name}': param '{param}' is empty")
    return [_iri(v, step_name, param) for v in values]


def _build_map_predicate(params: dict, hdt_file: str, out_file: str) -> str:
    """For every triple (s, FROM, o) in the HDT, emit (s, TO, o).

    `? P ?` searches use the .hdt.index.v1-1 written by `hdtc create --index`.
    The sed swap replaces the first occurrence of " <FROM> " per line: subjects
    are single tokens, so the predicate is always the first possible match and
    object literals are never touched. `&` is escaped because it is special in
    a sed replacement.
    """
    from_preds = _iri_list(params.get("from"), "map-predicate", "from")
    to_pred = _iri(params.get("to"), "map-predicate", "to").replace("&", r"\&")
    searches = "; ".join(
        f"hdtc search {hdt_file} --query '? <{p}> ?' | sed 's| <{p}> | <{to_pred}> |'"
        for p in from_preds
    )
    return f"set -euo pipefail; {{ {searches}; }} | gzip > {out_file}"


AUGMENTATION_CATALOG = {
    "map-predicate": _build_map_predicate,
}


def build_augmentation_jobs(steps, hdt_file: str, aug_dir: str) -> list:
    """Validate `steps` (objects with .name/.params) against the catalog.

    Returns [(job_suffix, out_file, bash_command)]; raises ValueError on an
    unknown name or bad params so callers can fail fast, before any job runs.
    """
    jobs = []
    for i, step in enumerate(steps):
        builder = AUGMENTATION_CATALOG.get(step.name)
        if builder is None:
            raise ValueError(
                f"unknown augmentation '{step.name}' "
                f"(known: {', '.join(sorted(AUGMENTATION_CATALOG))})"
            )
        out_file = f"{aug_dir}/{i:02d}-{step.name}.nt.gz"
        jobs.append((f"aug{i}", out_file, builder(step.params or {}, hdt_file, out_file)))
    return jobs


def build_merge_command(hdt_file: str, nt_file: str, aug_files: list,
                        temp_dir: str, memory_limit: str) -> str:
    """Fold the aug triples into both artifacts.

    `hdtc create` accepts HDT files as inputs, so the merge reuses the existing
    dictionary instead of reparsing; --index regenerates {hdt}.index.v1-1.
    Appending the gzipped aug files to nt/graph.nt.gz is valid gzip
    (concatenated members). Aug files are listed explicitly — never globbed —
    so stale files from a previous run cannot leak in.
    """
    aug = " ".join(aug_files)
    merged = f"{hdt_file}.merged"
    return (
        "set -euo pipefail; "
        f"hdtc create {hdt_file} {aug} --temp-dir {temp_dir} --index "
        f"--memory-limit {memory_limit} -v --output {merged}; "
        f"mv {merged} {hdt_file}; "
        f"mv {merged}.index.v1-1 {hdt_file}.index.v1-1; "
        f"cat {aug} >> {nt_file}"
    )
