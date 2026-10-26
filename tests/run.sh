#!/usr/bin/env bash
# Run every check in this directory.
#
#   tests/run.sh              # all of them
#   tests/run.sh kg_subset    # just the ones whose name matches
#
# These are plain assert scripts, not a pytest suite: each one is runnable on
# its own, exits non-zero on failure, and prints a line saying what it proved.
# They need the app's runtime dependencies importable (the kace image has them;
# locally, a venv with requirements.txt installed will do).
set -uo pipefail

HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ROOT="$(dirname "$HERE")"
PY="${PYTHON:-python3}"
export PYTHONPATH="$ROOT/src:${PYTHONPATH:-}"
# Config is a pydantic singleton built from env at import; a namespace is all
# these checks need from it.
export K8S_NAMESPACE="${K8S_NAMESPACE:-test}"

filter="${1:-}"
pass=0; fail=0; failed=()

for t in "$HERE"/test_*.py; do
  name="$(basename "$t" .py)"
  [ -n "$filter" ] && [[ "$name" != *"$filter"* ]] && continue
  printf '%-26s ' "$name"
  if out=$("$PY" "$t" 2>&1); then
    echo "${out##*$'\n'}"
    pass=$((pass+1))
  else
    echo "FAILED"
    echo "$out" | sed 's/^/    /' | tail -15
    fail=$((fail+1)); failed+=("$name")
  fi
done

echo
echo "$pass passed, $fail failed"
[ $fail -eq 0 ] || { printf '  failed: %s\n' "${failed[@]}"; exit 1; }
