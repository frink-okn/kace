# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## What KACE is

**KACE** (Kubernetes Artifacts Creation Engine) drives FRINK data pipelines: it receives webhooks from LakeFS (merge / tag-creation events), runs RDF/Neo4j format conversions as K8s Jobs, then deploys SPARQL endpoints (per-KG QLever, Fuseki, a federated QLever server) and an LDF aggregator on the cluster. Notifies via Slack + email.

Orchestration is **Temporal-only** — the legacy Celery stack (`src/server.py`, `src/celery_tasks/`) has been deleted. Some remnants linger (see "Stale remnants" below); don't resurrect them.

## Running locally

```bash
# 1. Backing services (Temporal server + Postgres + Temporal UI; redis service is vestigial)
docker-compose up -d         # Temporal UI: localhost:8080, Temporal gRPC: 7233

# 2. Python deps
pip install -r requirements.txt

# 3. Env
cp .env-template .env        # then fill in LakeFS creds, Slack, SMTP, etc.

# 4. Webhook server (FastAPI on :9899)
export PYTHONPATH=src
python src/temporal_server.py

# 5. Temporal worker (executes all workflows/activities; queue `frink-temporal-queue`)
python -m temporal_app.worker
```

There are no tests, lint config, or build script — `Dockerfile` just installs `requirements.txt` and copies `src/`. (`src/temporal_app/trigger_test.py` is a scratch script for manually starting workflows against a Temporal server, not a test suite.) Deployment is via the Helm chart in `helm-chart/kace/` (depends on the upstream `temporal` chart, disabled by default).

## High-level flow

```
LakeFS event → webhook (FastAPI) → Temporal workflow → K8s Job(s) (conversion)
                                                     ↓
                                       upload artifacts back to LakeFS
                                                     ↓
                                       tag creation → deploy SPARQL endpoint
                                                     ↓
                                       LDF aggregator sync → Slack/email canary
```

## Endpoints → workflows (`src/temporal_server.py`, port 9899)

| Endpoint | Workflow | Workflow ID |
|---|---|---|
| `POST /convert_to_hdt` | `HDTConversionWorkflow` | `hdt-{repo}` |
| `POST /convert_neo4j_to_hdt` | `Neo4jConversionWorkflow` | `neo4j-{repo}` |
| `POST /handle_tag_creation` | `QLeverDeploymentWorkflow` + kicks `LDFSyncWorkflow` | `qlever-deployment-{repo}`, `ldf-sync-{repo}` |
| `POST /sync_ldf` | `LDFSyncWorkflow` | `ldf-sync-{repo or "all"}` |
| `POST /trigger_qlever_index` | `QLeverIndexWorkflow` | `qlever-index-build` |
| `POST /trigger_qlever_federation_deploy` | `QLeverFederationDeploymentWorkflow` | `qlever-federation-deploy` |

Two workflows are registered in the worker but have **no HTTP trigger**: `DeploymentWorkflow` (Fuseki + LDF + spider job — legacy tag-creation path) and `FusekiDeploymentWorkflow` (download HDT + Fuseki + LDF). Start them manually via a Temporal client (see `trigger_test.py`) if needed.

## Pipeline details

**HDT conversion** (`workflows/hdt_conversion.py`, ~424 lines, the big one): download inputs (skipped if `files_list` passed in) → `hdtc-job` (HDT create) → NT merge job (`hdtc dump | gzip`) → RIOT validation job → VOID job → append pav version/date triples to `void.nt` (version from `get_future_tag`) → per-KG QLever index job (`IndexBuilderMain`, with hardcoded per-repo grep filters for `secure-chain-kg` / `spatial-kg`) → upload hdt/nt/void/qlever artifacts to LakeFS → also upload `void.nt` to the `config.void_repo` (`okn-void:develop`) → review email to KG owners → cleanup. Jobs run via `run_k8s_job` + `watch_k8s_job_sync` pairs.

**Neo4j conversion** (`workflows/neo4j_conversion.py`): export dump → JSON → RDF conversion job → then **chains into `HDTConversionWorkflow` as a child workflow**, passing `files_list=[.../nt/graph.nt.gz]` so the child skips download and converts the local NT file.

**LDF sync** (`workflows/ldf_sync.py`): per-KG — resolve latest tag/commit (`resolve_kg_ref`), compare against commit ids stored in a state ConfigMap, submit a K8s Job (`src/k8s/jobs/ldf_sync_kg.py` runs inside `config.ldf_sync_image`) to pull HDT into the shared LDF PVC, update state only on Job success. Afterwards render the LDF datasource ConfigMap (registry KGs + `config.ldf_extra_datasources`) and patch the deployment annotation for a rolling restart.

## Federated QLever index build (QLeverIndexWorkflow)

Builds one big qlever index over ~all KGs + s2 geo-augmentation graphs. Triggered by `POST /trigger_qlever_index` (workflow id `qlever-index-build`, single-flight, cancels prior).

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

The workflow does NOT do server rollover — it ends at "PVC populated + state updated".

Per-repo input filters (gunzip pipe suffixes) and per-repo lakefs overrides (alternate remote path / ref) live as module-level dicts in `activities.py` (`PER_REPO_INPUT_FILTERS`, `PER_REPO_LAKEFS_OVERRIDES`). Add an entry there rather than special-casing inside `prepare_qlever_job_specs`.

## Federation server rollover (QLeverFederationDeploymentWorkflow)

Consumes the index build's state: `resolve_qlever_federation_build_id` picks the PVC to serve — default `build_id_serving`, `use_previous=true` for n-1 rollback, explicit `build_id=` wins over both. `deploy_qlever_federation` then renders `templates/qlever-federation/*.j2` (via `qlever_federation_server_manager`) mounting that PVC into a qlever-server Deployment exposed at `/{config.qlever_federation_prefix}` (default `/federation`). Slack notified at start/success/failure.

## Key building blocks

- **`src/k8s/`** — the only place that talks to the cluster. `podman.JobMan` submits Jobs from templates in `src/k8s/templates/*.yaml`; the `job_type` string passed to `run_k8s_job` selects the template via the `mapping` dict at the top of `podman.py` (`hdtc-job`, `nt-merge-job`, `qlever-index-job`, `neo4j-rdf-job`, `neo4j-json-job`, `spider-job`, `void-job`, `documentation-job`). Four deployment managers render Deployment/Service/Ingress/PVC manifests from Jinja dirs resolved in `src/k8s/__init__.py`: `server_man.py` (Fuseki — also reused for the old Fuseki-federation templates in `templates/federation/`), `server_man_ldf.py` (LDF aggregator), `server_man_qlever.py` (per-KG QLever), `server_man_qlever_federation.py` (federated QLever, `templates/qlever-federation/`). `qlever_state.py` (ConfigMap-backed state) and `qlever_pvc.py` (per-build output PVC lifecycle) backstop the federated index build.
- **`src/temporal_app/`** — `activities.py` holds ALL activity implementations (~1000 lines); workflows in `workflows/*.py` are thin orchestrations over them. `worker.py` registers every workflow AND every activity.
- **`src/lakefs_util/io_util.py`** — wraps LakeFS client for download/upload, branch/tag creation, `clean_up_files(repo_id)`. `semver_util.py` handles tag ordering.
- **`src/models/`** — Pydantic models. `lakefs_models.py` defines webhook payloads (`LakefsMergeActionModel`, `LakefTagCreationModel`). `kg_metadata.py` defines `KGConfig` / `KG`, loaded from the okn-registry kgs.yaml at `config.kg_config_url` via `KGConfig.from_git()`.
- **`src/canary/`** — `slack.py` (`slack_canary.notify_event` / `send_slack_message` / `@slack_notify_on_failure` decorator) and `mail.py` (`mail_canary.send_review_email`).
- **`src/config.py`** — single `Config` pydantic singleton built from env vars.
- **`ToolsDocker/`** — Dockerfiles for conversion containers (`neo4j-json`, `neo4j-nt`, `spider-integration`). Job templates in `src/k8s/templates/` reference these images.

## Adding a new workflow (the pattern)

1. Implement activities in `temporal_app/activities.py` — all side effects (LakeFS, K8s, HTTP, fs) live there.
2. Create `temporal_app/workflows/<name>.py`; import activities under `with workflow.unsafe.imports_passed_through():`; call them with `workflow.execute_activity(..., start_to_close_timeout=..., retry_policy=...)`. For long K8s jobs pair `run_k8s_job` with `watch_k8s_job_sync` and set a `heartbeat_timeout`.
3. Export from `workflows/__init__.py` (import + `__all__`).
4. Register in `worker.py` — the workflow class AND every new activity. Temporal rejects unregistered activities at runtime; this is the most common miss.
5. Add an endpoint in `temporal_server.py`: deterministic workflow id, `cancel_existing_workflow()` before `start_workflow()` (preserves at-most-one-in-flight), task queue `frink-temporal-queue`.
6. New config: env var in `.env-template` + field in `src/config.py` + lowercase key in `helm-chart/kace/values.yaml` `app_config:` (keys are uppercased into container env by the `kace.env_variables` helper in `_helpers.tpl`).
7. New K8s resource kinds need an entry in the worker ServiceAccount Role (`helm-chart/kace/templates/role.yaml` — currently configmaps, PVCs, jobs, deployments, services, secrets, ingresses, httproutes, healthcheckpolicies).

## Conventions worth knowing

- **Per-repo workflow IDs + cancellation**: the webhook cancels any existing workflow with the same deterministic ID before starting a new one (`cancel_existing_workflow` in `temporal_server.py`) — this is how at-most-one-in-flight is enforced. Preserve this when adding new workflows.
- **Memory sizes**: webhook params take K8s style (`28Gi`); JVM/qlever flags want raw (`28G`). `temporal_server.py` strips the `i` before passing to workflows — replicate that pattern.
- **K8s templates are Jinja, not pure YAML**: `*.j2` files render in the server managers (Job templates `*.yaml` are plain YAML loaded by `JobMan`). Don't expect kubectl to lint the `.j2` ones.
- **Skipping repos**: `config.conversion_skip_repos` (comma-separated env) blocks certain LakeFS repos from auto-conversion.
- **`KGConfig.from_git()`** pulls live from GitHub on every call — cache it locally if you'd call it repeatedly in one workflow.
- **`download_file_lakefs(repo, remote_path, local_path, ref=None)`** — `ref=None` resolves latest tag inside the activity; passing an explicit ref pins the download. Workflows that want all parts of a single run pinned to one snapshot should resolve refs once up front (see `resolve_qlever_refs`).
- **`workflow.info().start_time` / `workflow.now()`** — only sources of time inside a workflow. Both deterministic across replays. Don't use `datetime.now()` / `time.time()` in workflow code; OK inside activities.

## Stale remnants / gotchas

- **Helm chart defaults are broken post-Celery-removal**: `values.yaml` ships `workerMode: "celery"` and `command.entry: "server:app"` — both reference deleted code. Real deployments must set `workerMode: "temporal"` and `command.entry: "temporal_server:app"`. The celery-worker + redis sidecar containers in `templates/deployment.yaml` are dead branches gated on `workerMode: celery`.
- **Port mismatch**: the chart's web-server container runs uvicorn on **9898**; running `python src/temporal_server.py` locally uses **9899** (its `__main__` block).
- `requirements.txt` still pins `celery` + `redis`, and `docker-compose.yaml` still starts a redis service — none of the code uses them.
- `.env-template` lags behind `src/config.py` (missing the LDF_*/QLEVER_* vars); the helm `app_config:` block is the complete mapping.
- `config.py` passes `slack_webhook_url=` to `Config(...)` but the model has no such field — pydantic silently ignores it; the Slack canary uses `slack_token`/`slack_channel`.
