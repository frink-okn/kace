"""Check that the live registry parses and that Slack calls name real methods.

Run: PYTHONPATH=src python3 tests/test_registry_parse.py

Both halves pin one production outage. A `kgf:` block was added to wikidata's
frink-options with no `lakefs-repo` / `documentation-path`; those fields were
declared `Optional[str] = Field(alias=...)` with no default, which in pydantic
v2 is REQUIRED, not optional. KGConfig.from_git() then raised for every caller
-- and since every webhook reads it, conversion and deployment broke for all
40+ KGs at once, from one KG's metadata-only edit.

The failure surfaced as an AttributeError instead of that ValidationError,
because the `except` handler reporting it called `slack_canary.send_slack_message`,
which has never existed (the method is `send_message`). The error path had
never run, so the typo sat there masking the real cause and returning 500.
"""
import ast
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT / "src"))

from canary.slack import SlackCanary
from models.kg_metadata import FrinkOptions, KG

# 1. frink-options carrying only keys this model does not know about must still
#    validate -- a KG that opts into no KACE feature is a normal registry entry.
opts = FrinkOptions(**{"kgf": {"semantics": {"roles": {"label": ["http://x/l"]}}}})
assert opts.lakefs_repo is None, opts.lakefs_repo
assert opts.documentation_path is None, opts.documentation_path
assert opts.augmentations == []

# 2. The keys that ARE known still bind through their aliases.
opts = FrinkOptions(**{
    "lakefs-repo": "scales-kg",
    "documentation-path": "scales-kg",
    "augmentations": [{"name": "map-predicate",
                       "params": {"from": ["http://x/a", "http://x/b"],
                                  "to": "http://x/label"}}],
})
assert opts.lakefs_repo == "scales-kg"
assert opts.augmentations[0].params["from"] == ["http://x/a", "http://x/b"]
# `from` as a bare string is equally valid -- most KGs use that form.
assert FrinkOptions(**{
    "augmentations": [{"name": "map-predicate",
                       "params": {"from": "http://x/a", "to": "http://x/label"}}],
}).augmentations[0].params["from"] == "http://x/a"

# 3. A KG whose frink-options is absent entirely is still a valid KG.
kg = KG(description="d", shortname="s")
assert kg.frink_options is None

# 4. Every slack_canary.<attr> the app calls must exist on SlackCanary. A typo
#    here only shows up on the error path, i.e. exactly when something else is
#    already going wrong.
called = set()
for src in (ROOT / "src").rglob("*.py"):
    for node in ast.walk(ast.parse(src.read_text(encoding="utf-8"))):
        if (isinstance(node, ast.Attribute)
                and isinstance(node.value, ast.Name)
                and node.value.id == "slack_canary"):
            called.add(node.attr)
assert called, "no slack_canary calls found -- did the import move?"
missing = sorted(a for a in called if not hasattr(SlackCanary, a))
assert not missing, f"SlackCanary has no {missing} (called somewhere in src/)"

print(f"registry parse: frink-options optional keys stay optional; "
      f"{len(called)} slack_canary method(s) all exist")
