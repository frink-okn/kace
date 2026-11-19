"""Check the HTTPRoutes point at the nginx throttle only when asked to.

Run: PYTHONPATH=src python tests/test_throttle_routes.py

With throttle_enabled the Gateway must hand the ORIGINAL path to nginx (no
URLRewrite -- nginx does the rewrite and sets the per-KG Host header itself).
If the route kept rewriting to "/", nginx could not tell which KG a request is
for. With it off, output must be the pre-throttle route, so a rollback via
THROTTLE_ENABLED=false really restores the old path (interceptor / federation).
"""
from pathlib import Path

import yaml
from jinja2 import Environment, FileSystemLoader

T = Path(__file__).resolve().parent.parent / "src/k8s/templates"


def render(d, **params):
    env = Environment(loader=FileSystemLoader(T / d), trim_blocks=True, lstrip_blocks=True)
    return yaml.safe_load(env.get_template("httproute.j2").render(**params))["spec"]["rules"][0]


common = dict(kg_name="kg1", host_name="h", federation_prefix="federation")
on = dict(common, throttle_enabled=True, throttle_service="frink-throttle", throttle_port=8080)

for d in ("qlever", "qlever-federation"):
    rule = render(d, **on)
    assert rule["backendRefs"] == [{"name": "frink-throttle", "port": 8080}], d
    assert "filters" not in rule, d
    assert "namespace" not in rule["backendRefs"][0], d  # same ns: no ReferenceGrant

kg = render("qlever", **common)
assert kg["backendRefs"][0]["name"] == "keda-add-ons-http-interceptor-proxy"
assert kg["filters"][0]["urlRewrite"]["hostname"] == "kg1.qlever.frink.internal"
fed = render("qlever-federation", **common)
assert fed["backendRefs"][0]["name"] == "frink-federation-qlever-server"
assert fed["filters"][0]["urlRewrite"]["path"]["replacePrefixMatch"] == "/"

print("throttle routes: on -> nginx w/o rewrite, off -> unchanged")
