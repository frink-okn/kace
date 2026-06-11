# KACE

KACE (<u>K</u>ubernetes <u>A</u>rtifacts <u>C</u>reation <u>E</u>ngine) drives the FRINK data
pipelines. It receives webhooks from LakeFS (merge / tag-creation events), runs RDF and Neo4j
format conversions as Kubernetes Jobs, and deploys SPARQL endpoints (Fuseki, QLever) plus an
LDF aggregator on the cluster. Orchestration is built on [Temporal](https://temporal.io/),
and notifications go out via Slack and email.

## Features

* Conversion of RDF formats to HDT (`HDTConversionWorkflow`)
* Conversion of Neo4j dumps to HDT (`Neo4jConversionWorkflow`)
* Deployment of Fuseki SPARQL endpoints (`FusekiDeploymentWorkflow`)
* Deployment of per-KG QLever endpoints (`QLeverDeploymentWorkflow`)
* Federated QLever index build over all KGs (`QLeverIndexWorkflow`)
* Deployment/rollover of the federated QLever server (`QLeverFederationDeploymentWorkflow`)
* LDF aggregator sync and rolling restart (`LDFSyncWorkflow`)
* Liveness probing after deployment, Slack/email notifications on workflow events

## High-level flow

```
LakeFS event → webhook (FastAPI) → Temporal workflow → K8s Job (conversion)
                                                     ↓
                                       upload artifacts back to LakeFS
                                                     ↓
                                       tag creation → deploy SPARQL endpoint
                                                     ↓
                                       LDF aggregator sync → Slack/email canary
```

## Running locally

```bash
# 1. Backing services (Temporal server + Postgres + Temporal UI + Redis)
docker-compose up -d         # Temporal UI: localhost:8080, Temporal gRPC: 7233

# 2. Python deps
pip install -r requirements.txt

# 3. Env
cp .env-template .env        # fill in LakeFS creds, Slack, SMTP, etc.

# 4. Webhook server (FastAPI, port 9899)
export PYTHONPATH=src
python src/temporal_server.py

# 5. Temporal worker (required — executes all workflows/activities)
python -m temporal_app.worker    # task queue: frink-temporal-queue
```

There are no tests or lint config — the `Dockerfile` installs `requirements.txt` and copies
`src/`. Cluster deployment is via the Helm chart in `helm-chart/kace/` (depends on the
upstream `temporal` chart).

## Webhook endpoints (`src/temporal_server.py`)

| Endpoint | Workflow | Workflow ID |
|---|---|---|
| `POST /convert_to_hdt` | `HDTConversionWorkflow` | `hdt-{repo}` |
| `POST /convert_neo4j_to_hdt` | `Neo4jConversionWorkflow` | `neo4j-{repo}` |
| `POST /handle_tag_creation` | `QLeverDeploymentWorkflow` (+ `LDFSyncWorkflow`) | `qlever-deployment-{repo}`, `ldf-sync-{repo}` |
| `POST /sync_ldf` | `LDFSyncWorkflow` | `ldf-sync-{repo or "all"}` |
| `POST /trigger_qlever_index` | `QLeverIndexWorkflow` | `qlever-index-build` |
| `POST /trigger_qlever_federation_deploy` | `QLeverFederationDeploymentWorkflow` | `qlever-federation-deploy` |

Workflow IDs are deterministic per repo. Before starting a workflow, the webhook cancels any
in-flight workflow with the same ID (`cancel_existing_workflow`) — this enforces
at-most-one-in-flight per repo.

## Repository layout

| Path | Purpose |
|---|---|
| `src/temporal_server.py` | FastAPI webhook server (port 9899) |
| `src/temporal_app/worker.py` | Temporal worker; registers every workflow and activity |
| `src/temporal_app/workflows/` | One file per workflow — thin orchestration over activities |
| `src/temporal_app/activities.py` | All activity implementations (LakeFS I/O, K8s job submission, state, notifications) |
| `src/k8s/` | The only place that talks to the cluster. `podman.JobMan` submits Jobs from Jinja templates in `src/k8s/templates/`; `server_man*.py` render Deployment/Service/Ingress/PVC manifests for Fuseki, LDF, QLever, and the QLever federation server; `qlever_state.py` / `qlever_pvc.py` back the federated index build |
| `src/lakefs_util/io_util.py` | LakeFS client wrapper — download/upload, branch/tag creation, cleanup |
| `src/models/` | Pydantic models: webhook payloads (`lakefs_models.py`) and KG registry config (`kg_metadata.py`) |
| `src/canary/` | Slack (`slack.py`) and email (`mail.py`) notifications |
| `src/config.py` | Single `Config` pydantic singleton built from env vars |
| `ToolsDocker/` | Dockerfiles for conversion containers (`neo4j-nt`, `neo4j-json`, `spider-integration`) referenced by the Job templates |
| `helm-chart/kace/` | Helm chart for cluster deployment (ConfigMap mirrors `.env-template`, RBAC role, etc.) |

## Adding a new workflow

Follow this pattern (every existing workflow does):

1. **Write activities** in `src/temporal_app/activities.py`. Activities are where all side
   effects live — LakeFS calls, K8s API calls, HTTP, filesystem. Workflow code must stay
   deterministic: use `workflow.now()` / `workflow.info().start_time` for time, never
   `datetime.now()` or `time.time()` (those are fine *inside* activities).

2. **Create the workflow** in `src/temporal_app/workflows/<name>.py`:

   ```python
   from temporalio import workflow
   from datetime import timedelta
   from temporalio.common import RetryPolicy

   with workflow.unsafe.imports_passed_through():
       from ..activities import my_activity   # import activities this way

   @workflow.defn
   class MyNewWorkflow:
       @workflow.run
       async def run(self, repo_id: str) -> dict:
           return await workflow.execute_activity(
               my_activity,
               repo_id,
               start_to_close_timeout=timedelta(minutes=10),
               retry_policy=RetryPolicy(maximum_attempts=3),
           )
   ```

   For long-running K8s jobs, pair a long `start_to_close_timeout` with a
   `heartbeat_timeout` and an activity that heartbeats while polling
   (see `watch_k8s_job_sync` usage in `qlever_index.py`).

3. **Export it** from `src/temporal_app/workflows/__init__.py` (both the import and
   `__all__`).

4. **Register it in `src/temporal_app/worker.py`** — add the workflow class to the
   `workflows=[...]` list and *every* new activity to the `activities=[...]` list.
   Temporal rejects unregistered activities at runtime; this is the most common miss.

5. **Add a webhook endpoint** in `src/temporal_server.py`:
   * Pick a deterministic workflow ID (e.g. `myflow-{repo_id}`).
   * Call `cancel_existing_workflow(client, workflow_id)` before
     `client.start_workflow(...)` to preserve the at-most-one-in-flight convention.
   * Memory sizes from webhook params arrive K8s-style (`28Gi`); strip the `i` before
     passing to anything that wants JVM/qlever-style values (`28G`) — see existing
     endpoints for the pattern.

6. **Configuration**: anything new and deployment-related needs an env var in
   `.env-template`, a matching field in `src/config.py`, and the same var mapped in the
   `helm-chart/kace/values.yaml` ConfigMap.

7. **K8s resources**: Job/Deployment manifests are Jinja templates (`*.j2`) under
   `src/k8s/templates/` — rendered by `podman.JobMan` or a server manager, so kubectl
   can't lint them directly. If the workflow touches a new K8s resource *kind*, add it to
   the worker ServiceAccount Role in `helm-chart/kace/templates/role.yaml`.

## Conventions worth knowing

* `config.conversion_skip_repos` (comma-separated env var) blocks specific LakeFS repos
  from auto-conversion.
* `KGConfig.from_git()` pulls the okn-registry `kgs.yaml` live from GitHub on every call —
  cache the result if used repeatedly inside one workflow run.
* `download_file_lakefs(repo, remote_path, local_path, ref=None)`: `ref=None` resolves the
  latest tag inside the activity; pass an explicit ref to pin. Workflows that need all
  steps pinned to one snapshot should resolve refs once up front (see `resolve_qlever_refs`).
* Per-repo exceptions for the federated QLever build (input filters, alternate LakeFS
  paths/refs) live as module-level dicts in `activities.py`
  (`PER_REPO_INPUT_FILTERS`, `PER_REPO_LAKEFS_OVERRIDES`).
