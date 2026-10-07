#!/usr/bin/env python3
"""Repoint the live QLever HTTPRoutes at the frink-throttle nginx proxy (or back).

Why this exists: routes are only re-rendered on a KG's next tag, so flipping
THROTTLE_ENABLED alone leaves the running fleet unthrottled. This patches the
existing routes in place. Mirrors qlever/httproute.j2 and
qlever-federation/httproute.j2 -- keep the three in sync.

  to throttle: backendRefs -> frink-throttle, URLRewrite filter dropped (nginx
               rewrites the path and sets the per-KG Host itself).
  --revert:    the pre-throttle shape (KEDA interceptor + Host rewrite for a KG,
               the federation Service + path rewrite for federation).

A route is only touched when it is in one of those two known shapes. Anything
else (extra rules, another backend, a Host rewrite that does not match its
path) is reported and left alone: nginx derives the Host from the PATH, so a
route whose current Host differs from its path would silently start hitting a
different KG.

Idempotent. Dry run unless --apply. Roll out in batches:
  --only federation      then      --only kg1,kg2,...      then everything.
"""
import argparse
import json
import subprocess

FED_ROUTE = "frink-federation-qlever-route"
FED_SERVICE = "frink-federation-qlever-server"
INTERCEPTOR = "keda-add-ons-http-interceptor-proxy"
THROTTLE = ("frink-throttle", 8080)


def kg_of(route_name):
    return route_name[len("frink-"):-len("-qlever-route")]


def route_name_for(kg):
    return FED_ROUTE if kg == "federation" else f"frink-{kg}-qlever-route"


def backend(route_name, throttle):
    """(backendRefs, filters) a route should carry. filters None = none at all."""
    if throttle:
        return [{"name": THROTTLE[0], "port": THROTTLE[1]}], None
    if route_name == FED_ROUTE:
        return ([{"name": FED_SERVICE, "port": 7001}],
                [{"type": "URLRewrite", "urlRewrite": {"path": {
                    "type": "ReplacePrefixMatch", "replacePrefixMatch": "/"}}}])
    return ([{"name": INTERCEPTOR, "namespace": "keda", "port": 8080}],
            [{"type": "URLRewrite", "urlRewrite": {
                "hostname": f"{kg_of(route_name)}.qlever.frink.internal",
                "path": {"type": "ReplacePrefixMatch", "replacePrefixMatch": "/"}}}])


def state(route):
    """'throttle', 'original', or 'unknown: <why>'.

    Decided by backend NAME, not by comparing whole objects: the API server
    fills in defaults (group, kind, weight) on every backendRef, so a live route
    never equals the dict we patched in.
    """
    name, ns = route["metadata"]["name"], route["metadata"].get("namespace")
    rules = route["spec"].get("rules", [])
    if len(rules) != 1 or len(rules[0].get("backendRefs", [])) != 1:
        return "unknown: not exactly one rule with one backend"
    rule, ref = rules[0], rules[0]["backendRefs"][0]
    if ref["name"] == THROTTLE[0] and ref.get("namespace", ns) == ns:
        return "throttle"
    prefix = "/federation" if name == FED_ROUTE else f"/{kg_of(name)}"
    paths = [m.get("path", {}).get("value") for m in rule.get("matches", [])]
    if paths != [prefix]:
        return f"unknown: path {paths}, expected [{prefix!r}]"
    if name == FED_ROUTE:
        return "original" if ref["name"] == FED_SERVICE else f"unknown: backend {ref['name']}"
    if ref["name"] != INTERCEPTOR or ref.get("namespace") != "keda":
        return f"unknown: backend {ref.get('namespace', ns)}/{ref['name']}"
    hosts = [f["urlRewrite"].get("hostname") for f in rule.get("filters", [])
             if f["type"] == "URLRewrite"]
    want = f"{kg_of(name)}.qlever.frink.internal"
    return "original" if hosts == [want] else f"unknown: Host rewrite {hosts}, expected [{want!r}]"


def patch_ops(route, throttle):
    """JSON-patch ops; [] if already in the wanted shape; None if not ours to touch."""
    s = state(route)
    if s.startswith("unknown"):
        return None
    if s == ("throttle" if throttle else "original"):
        return []
    refs, filters = backend(route["metadata"]["name"], throttle)
    ops = [{"op": "replace", "path": "/spec/rules/0/backendRefs", "value": refs}]
    if filters is None:
        if "filters" in route["spec"]["rules"][0]:
            ops.append({"op": "remove", "path": "/spec/rules/0/filters"})
    else:
        ops.append({"op": "add", "path": "/spec/rules/0/filters", "value": filters})
    return ops


def kubectl(*args, ns=None, ctx=None):
    cmd = ["kubectl"] + (["--context", ctx] if ctx else []) + (["-n", ns] if ns else [])
    return subprocess.run(cmd + list(args), capture_output=True, text=True,
                          check=True).stdout


def self_test():
    def live(name, refs, filters, path):
        """A route as the API server returns it: defaults filled in on backendRefs."""
        refs = [{"group": "", "kind": "Service", "weight": 1, **r} for r in refs]
        rule = {"backendRefs": refs,
                "matches": [{"path": {"type": "PathPrefix", "value": path}}]}
        if filters is not None:
            rule["filters"] = filters
        return {"metadata": {"name": name, "namespace": "frink"}, "spec": {"rules": [rule]}}

    kg, fed = "frink-sawgraph-qlever-route", FED_ROUTE
    for name, path in ((kg, "/sawgraph"), (fed, "/federation")):
        orig = live(name, *backend(name, False), path)
        assert state(orig) == "original", state(orig)
        assert patch_ops(orig, throttle=False) == []
        ops = patch_ops(orig, throttle=True)
        assert [o["op"] for o in ops] == ["replace", "remove"], ops
        # The patched route, as the API server would return it, reads as throttled
        # (the defaults it adds must not make it look unknown or unpatched).
        thr = live(name, ops[0]["value"], None, path)
        assert state(thr) == "throttle", state(thr)
        assert patch_ops(thr, throttle=True) == []
        back = patch_ops(thr, throttle=False)
        assert back == [{"op": "replace", "path": "/spec/rules/0/backendRefs",
                         "value": backend(name, False)[0]},
                        {"op": "add", "path": "/spec/rules/0/filters",
                         "value": backend(name, False)[1]}], back

    # Shapes this script must refuse to touch.
    wrong_host = live(kg, *backend("frink-other-qlever-route", False), "/sawgraph")
    assert patch_ops(wrong_host, True) is None, state(wrong_host)
    direct = live(kg, [{"name": "frink-sawgraph-qlever-server", "port": 7001}], None, "/sawgraph")
    assert patch_ops(direct, True) is None, state(direct)
    two_rules = live(kg, *backend(kg, False), "/sawgraph")
    two_rules["spec"]["rules"].append(two_rules["spec"]["rules"][0])
    assert patch_ops(two_rules, True) is None
    assert route_name_for("federation") == FED_ROUTE
    assert route_name_for("spoke-okn") == "frink-spoke-okn-qlever-route"
    print("self-test ok")


def main():
    global THROTTLE
    p = argparse.ArgumentParser()
    p.add_argument("-n", "--namespace", help="serving namespace (frink on GKE)")
    p.add_argument("--context", help="kubectl context of the serving cluster")
    p.add_argument("--service", default=THROTTLE[0])
    p.add_argument("--port", type=int, default=THROTTLE[1])
    p.add_argument("--only", help="comma-separated KG names ('federation' for the federated "
                                  "route); exact names, default all")
    p.add_argument("--revert", action="store_true", help="point routes back at the interceptor / federation")
    p.add_argument("--apply", action="store_true", help="actually patch; default is dry run")
    p.add_argument("--self-test", action="store_true")
    a = p.parse_args()
    THROTTLE = (a.service, a.port)

    if a.self_test:
        return self_test()
    if not a.namespace:
        p.error("-n/--namespace is required")

    items = json.loads(kubectl("get", "httproute", "-o", "json", ns=a.namespace, ctx=a.context))["items"]
    routes = {r["metadata"]["name"]: r for r in items
              if r["metadata"]["name"].endswith("-qlever-route")}
    if a.only:
        wanted = [route_name_for(k.strip()) for k in a.only.split(",") if k.strip()]
        missing = [w for w in wanted if w not in routes]
        if missing:
            p.error(f"no such route(s): {', '.join(missing)}")
        routes = {w: routes[w] for w in wanted}

    todo = skipped = 0
    for name, r in sorted(routes.items()):
        ops = patch_ops(r, throttle=not a.revert)
        if ops is None:
            skipped += 1
            print(f"LEAVE   {name}  ({state(r)})")
        elif not ops:
            print(f"ok      {name}")
        elif a.apply:
            todo += 1
            kubectl("patch", "httproute", name, "--type=json", "-p", json.dumps(ops),
                    ns=a.namespace, ctx=a.context)
            print(f"patched {name}")
        else:
            todo += 1
            print(f"would   {name}")
    print(f"\n{todo} to patch, {len(routes) - todo - skipped} already in place, "
          f"{skipped} left alone (unexpected shape)."
          + ("" if a.apply else "  Re-run with --apply."))


if __name__ == "__main__":
    main()
