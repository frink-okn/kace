"""Check that container images are pinned and configurable.

Run: PYTHONPATH=src python tests/test_images.py

The per-KG QLever servers ran `adfreiburg/qlever:latest`. A QLever server can
only read an index whose on-disk format its build understands, so an unpinned
tag turns any upstream push into an unannounced, fleet-wide format change --
discovered when a pod restarts and fails to load an index built months ago.
"""
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT / "src"))

TEMPLATES = ROOT / "src/k8s/templates"


def main():
    # 1. no literal image may float on a mutable tag. A templated value
    #    ({{ ... }}) is resolved from config, which is checked separately below.
    floating = []
    for f in list(TEMPLATES.rglob("*.j2")) + list(TEMPLATES.rglob("*.yaml")):
        for i, line in enumerate(f.read_text().splitlines(), 1):
            s = line.strip()
            if not s.startswith("image:"):
                continue
            value = s.split("image:", 1)[1].strip()
            if "{{" in value:            # rendered from config
                continue
            if value.endswith(":latest") or ":" not in value:
                floating.append(f"{f.relative_to(ROOT)}:{i} {s}")
    assert not floating, "unpinned image tags:\n  " + "\n  ".join(floating)

    # 2. both server templates take their image from config, not a literal
    for tmpl in ("qlever/server-deployment.j2", "qlever-federation/server-deployment.j2"):
        text = (TEMPLATES / tmpl).read_text()
        assert "{{ qlever_image }}" in text, f"{tmpl} does not render a configurable image"

    # 3. deploy_qlever must actually pass that parameter, or the render is empty
    acts = (ROOT / "src/temporal_app/activities.py").read_text()
    deploy = acts[acts.index("async def deploy_qlever("):]
    deploy = deploy[:deploy.index("\n@activity.defn")]
    assert '"qlever_image": config.qlever_server_image' in deploy, \
        "deploy_qlever does not pass qlever_image; the Deployment would render with no image"

    # 4. indexer and server images are separate knobs -- an index built by one
    #    build is not necessarily readable by another
    from config import config
    assert config.qlever_server_image and config.qlever_image
    assert ":latest" not in config.qlever_server_image
    assert ":latest" not in config.qlever_image

    # 5. BOTH index paths must take the indexer image from config. The per-KG
    #    job used to inherit whatever literal sat in qlever-index-job.yaml, so
    #    the indexer version was configurable on the federated path only.
    hdt = (ROOT / "src/temporal_app/workflows/hdt_conversion.py").read_text()
    assert "image=app_config.qlever_image" in hdt, \
        "the per-KG index job no longer passes a configured indexer image"
    fed = (ROOT / "src/temporal_app/workflows/qlever_index.py").read_text()
    assert "app_config.qlever_image" in fed

    # 6. Indexes are only readable by a server whose format matches the build
    #    that wrote them, so a pin is only a pin if both sides are pinned.
    assert config.qlever_image.startswith("adfreiburg/qlever:commit-"), config.qlever_image
    assert config.qlever_server_image.startswith("adfreiburg/qlever:commit-"), config.qlever_server_image

    print(f"image checks passed (server {config.qlever_server_image}, indexer {config.qlever_image})")


if __name__ == "__main__":
    main()
