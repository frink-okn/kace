# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## What KACE is

**KACE** (Kubernetes Artifacts Creation Engine) drives FRINK data pipelines: it receives webhooks from LakeFS (merge / tag-creation events), runs RDF/Neo4j format conversions as K8s Jobs, then deploys SPARQL endpoints (Fuseki, QLever) and an LDF aggregator on the cluster. Notifies via Slack + email.

## Running locally

```bash
# 1. Backing services (Redis broker + Temporal server + Postgres + Temporal UI)
docker-compose up -d         # Temporal UI: localhost:8080, Temporal gRPC: 7233, Redis: 6374

# 2. Python deps
pip install -r requirements.txt

# 3. Env
cp .env-template .env        # then fill in LakeFS creds, Slack, SMTP, etc.

# 4. Run a webhook server (pick one stack — see "Two parallel stacks" below)
export PYTHONPATH=src
python src/server.py             # legacy Celery-backed FastAPI on :9898
python src/temporal_server.py    # Temporal-backed FastAPI on :9899

# 5. Run the Temporal worker (required for temporal_server.py)
python -m temporal_app.worker

# 6. (Legacy stack only) run Celery worker
celery -A celery_tasks.celery worker --loglevel=info
```

There are no tests, lint config, or build script — `Dockerfile` just installs `requirements.txt` and copies `src/`. Deployment is via the Helm chart in `helm-chart/kace/` (depends on the upstream `temporal` chart).

## Two parallel stacks (important)

The repo is **mid-migration from Celery to Temporal**. Both stacks coexist and largely mirror each other:

| Concern              | Legacy (Celery)                              | New (Temporal)                                              |
|----------------------|----------------------------------------------|-------------------------------------------------------------|
| Webhook entrypoint   | `src/server.py` (port 9898)                  | `src/temporal_server.py` (port 9899)                        |
| Task definitions     | `src/celery_tasks/celery.py`                 | `src/temporal_app/activities.py` + `workflows/*.py`         |
| Worker               | `celery -A celery_tasks.celery worker`       | `python -m temporal_app.worker` (queue `frink-temporal-queue`) |
| Concurrency control  | none                                         | webhook cancels prior workflow per repo before starting     |

When adding new pipeline logic prefer the Temporal side. Workflows live in `src/temporal_app/workflows/` — each one (`HDTConversionWorkflow`, `Neo4jConversionWorkflow`, `QLeverDeploymentWorkflow`, `FusekiDeploymentWorkflow`, `QLeverIndexWorkflow`, `LDFSyncWorkflow`, `DeploymentWorkflow`) is a thin orchestration over activities in `activities.py`. All activities must be registered in `worker.py` or Temporal will reject them at runtime.

## High-level flow

```
LakeFS event → webhook (FastAPI) → workflow/task → K8s Job (conversion)
                                                ↓
                                  upload artifacts back to LakeFS
                                                ↓
                                  tag creation → deploy SPARQL endpoint
                                                ↓
                                  LDF aggregator sync → Slack/email canary
```

Key building blocks:

- **`src/k8s/`** — the only place that talks to the cluster. `podman.JobMan` submits Jobs from Jinja templates in `src/k8s/templates/`. Three deployment managers (`server_man.py` for Fuseki/federation, `server_man_ldf.py` for LDF aggregator, `server_man_qlever.py` for QLever) render Deployment/Service/Ingress/PVC manifests from `templates/{fuseki,ldf,qlever}/*.j2`. Template dirs are resolved in `src/k8s/__init__.py`. Two extra K8s helpers backstop the federated qlever index build: `qlever_state.py` (ConfigMap-backed state) and `qlever_pvc.py` (per-build output PVC lifecycle).
- **`src/lakefs_util/io_util.py`** — wraps LakeFS client for download/upload, branch/tag creation, `clean_up_files(repo_id)`.
- **`src/models/`** — Pydantic models. `lakefs_models.py` defines webhook payloads (`LakefsMergeActionModel`, `LakefTagCreationModel`). `kg_metadata.py` defines `KGConfig` / `KG`, loaded from the okn-registry kgs.yaml at `config.kg_config_url` via `KGConfig.from_git()`.
- **`src/canary/`** — `slack.py` (`slack_canary.notify_event` / `send_slack_message` / `@slack_notify_on_failure` decorator) and `mail.py` (`mail_canary.send_review_email`).
- **`src/config.py`** — single `Config` pydantic singleton built from env vars. Anything new in deployment needs both an env var in `.env-template` AND a field added here. The `helm-chart/kace/values.yaml` ConfigMap maps the same vars.
- **`ToolsDocker/`** — Dockerfiles for the actual conversion containers (`qEndpoint`, `neo4j-nt`, `neo4j-json`, `schema-gen`, `spider-integration`). Job templates in `src/k8s/templates/` reference these images.

## Federated QLever index build (QLeverIndexWorkflow)

Builds one big qlever index over ~all KGs + s2 geo-augmentation graphs. Triggered by `POST /trigger_qlever_index` on the temporal server (workflow id `qlever-index-build`, single-flight, cancels prior).

Layout:

- **Source files** on the existing `kace-local-pvc` (RWX), under `{config.qlever_source_path}` (default `/shared/qlever-source/`). One file per KG at `{repo}/graph.nt.gz`. s2 graphs at `s2/{file}`. Persistent across runs — change detection reuses them.
- **Output index** on a NEW per-build PVC `kace-qlever-index-{YYYYMMDD-HHMMSS}` (RWO, `premium-rwo`, default 3Ti). PVC is keyed by `build_id = workflow.info().start_time.strftime(...)` — deterministic across replays.
- **State ConfigMap** `kace-qlever-state`: `build_id_serving`, `build_id_previous`, `previous_marked_at`, `source_commits.json`, `image`. Used for both change-detect and GC.

Workflow phases (`src/temporal_app/workflows/qlever_index.py`):
1. Read state + GC output PVCs (orphans always; previous after `qlever_index_previous_ttl_hours`, default 24h).
2. `resolve_qlever_refs` — latest semver tag (fallback `main`) per repo + s2-builds tag. Wikidata always `main`. `PER_REPO_LAKEFS_OVERRIDES` in `activities.py` carries the per-repo exceptions.
3. Change-detect: if `current_commits == state.source_commits` and image unchanged and no `only_kg`, skip rebuild.
4. Create per-build output PVC.
5. Download every source file in parallel, pinned to resolved ref (no latest-tag resolution inside the activity once refs are computed).
6. Submit `qlever-index-{build_id}` Job. `JobMan.run_job` accepts `image=` (overrides template), `extra_pvcs=[{name,claim,mount_path,read_only}]` (mounts the per-build output PVC at `/index`), `read_only_default_mount=True` (source PVC mounted read-only).
7. `watch_k8s_job_sync` heartbeats every poll; workflow side sets `heartbeat_timeout=5min` + `start_to_close_timeout=7d` so the multi-day build survives worker restarts.
8. Write new state (`previous = old_serving`, `previous_marked_at = workflow.now()`), Slack canary.

The workflow does NOT do server rollover. It ends at "PVC populated + state updated". Downstream qlever-server-deployment workflow is the contract consumer of `state.build_id_serving`.

Per-repo input filters (gunzip pipe suffixes) and per-repo lakefs overrides (alternate remote path / ref) live as module-level dicts in `activities.py` (`PER_REPO_INPUT_FILTERS`, `PER_REPO_LAKEFS_OVERRIDES`). Add an entry there rather than special-casing inside `prepare_qlever_job_specs`.

## Conventions worth knowing

- **Per-repo workflow IDs**: Temporal webhooks use deterministic IDs like `hdt-{repo}`, `qlever-deployment-{repo}`, `ldf-sync-{repo}`. The webhook cancels any existing workflow with that ID before starting a new one (`cancel_existing_workflow` in `temporal_server.py`) — this is how at-most-one-in-flight is enforced. Preserve this when adding new workflows.
- **Memory sizes**: webhook params take K8s style (`28Gi`); JVM/qlever flags want raw (`28G`). `temporal_server.py` strips the `i` before passing to workflows — replicate that pattern.
- **K8s templates are Jinja, not pure YAML**: `*.j2` files render in `podman.JobMan` / the server managers. Don't expect kubectl to lint them directly.
- **Skipping repos**: `config.conversion_skip_repos` (comma-separated env) blocks certain LakeFS repos from auto-conversion.
- **`KGConfig.from_git()`** pulls live from GitHub on every call — cache it locally if you'd call it repeatedly in one workflow.
- **`download_file_lakefs(repo, remote_path, local_path, ref=None)`** — `ref=None` resolves latest tag inside the activity; passing an explicit ref pins the download. Workflows that want all parts of a single run pinned to one snapshot should resolve refs once up front (see `resolve_qlever_refs`).
- **`workflow.info().start_time` / `workflow.now()`** — only sources of time inside a workflow. Both deterministic across replays. Don't use `datetime.now()` / `time.time()` in workflow code; OK inside activities.
- **K8s RBAC**: the worker ServiceAccount Role (`helm-chart/kace/templates/role.yaml`) covers configmaps, PVCs, jobs, deployments, services, secrets, ingresses, httproutes, healthcheckpolicies. New resource kinds need an entry.
