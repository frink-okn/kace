"""Failures must reach Slack. Run: PYTHONPATH=src python tests/test_failure_alerts.py

Prevents: the fda-* repos (Nov 2026). Their merges hit /convert_to_hdt before
their okn-registry entries existed; the endpoint 500'd on a None registry
record, started no workflow, and nobody knew until an owner asked by email.
Workflow failures were equally silent for most workflows.

Proves:
  - unknown repo -> 404 + Slack, not an opaque 500;
  - any other unhandled endpoint error -> 500 + Slack;
  - a failed workflow posts one Slack alert carrying the activity's own error;
  - a cancelled workflow (every webhook re-fire cancels the old run) posts none;
  - a failed child posts once, not once per level.

Starts Temporal's time-skipping test server (downloaded on first run).
"""
import asyncio
import uuid
from datetime import timedelta

from fastapi.testclient import TestClient
from temporalio import activity, workflow
from temporalio.common import RetryPolicy
from temporalio.exceptions import ApplicationError
from temporalio.testing import WorkflowEnvironment
from temporalio.worker import UnsandboxedWorkflowRunner, Worker

import temporal_server
from config import config
from models.kg_metadata import KGConfig
from temporal_app.interceptors import SlackFailureInterceptor, should_alert

sent = []

# --- webhook ---------------------------------------------------------------

def check_webhook():
    config.webhook_token = "t"
    temporal_server.slack_canary.send_message = sent.append
    client = TestClient(temporal_server.app, raise_server_exceptions=False)
    body = {k: "x" for k in ("event_type", "event_time", "action_name", "hook_id", "branch_id",
                             "source_ref", "commit_message", "commit_id", "committer")}
    body.update(repository_id="fda-new-kg", commit_metadata={})

    async def empty_registry():
        return KGConfig(kgs=[])
    KGConfig.from_git = staticmethod(empty_registry)
    r = client.post("/convert_to_hdt", json=body, headers={"X-KACE-Token": "t"})
    assert r.status_code == 404, r.status_code
    assert len(sent) == 1 and "fda-new-kg" in sent[0] and "registry" in sent[0], sent

    async def broken_registry():
        raise RuntimeError("github down")
    KGConfig.from_git = staticmethod(broken_registry)
    sent.clear()
    r = client.post("/convert_to_hdt", json=body, headers={"X-KACE-Token": "t"})
    assert r.status_code == 500, r.status_code
    assert len(sent) == 1 and "github down" in sent[0] and "/convert_to_hdt" in sent[0], sent

    sent.clear()
    r = client.post("/convert_to_hdt", json=body, headers={"X-KACE-Token": "wrong"})
    assert r.status_code == 401 and not sent, "auth rejections must not page Slack"

# --- workflows -------------------------------------------------------------

@activity.defn(name="notify_slack")
async def fake_notify(message: str) -> None:
    sent.append(message)

@activity.defn
async def explode() -> None:
    raise ApplicationError("hdtc OOMKilled", non_retryable=True)

@workflow.defn
class Failing:
    @workflow.run
    async def run(self) -> None:
        await workflow.execute_activity(explode, start_to_close_timeout=timedelta(seconds=10),
                                        retry_policy=RetryPolicy(maximum_attempts=1))

@workflow.defn
class Parent:
    @workflow.run
    async def run(self) -> None:
        await workflow.execute_child_workflow(Failing.run, id=f"child-{uuid.uuid4()}")

@workflow.defn
class Waits:
    @workflow.run
    async def run(self) -> None:
        await workflow.wait_condition(lambda: False)


async def check_workflows():
    async with await WorkflowEnvironment.start_time_skipping() as env:
        async with Worker(env.client, task_queue="q", workflows=[Failing, Parent, Waits],
                          activities=[fake_notify, explode], interceptors=[SlackFailureInterceptor()],
                          workflow_runner=UnsandboxedWorkflowRunner()):
            async def outcome(wf, cancel=False):
                sent.clear()
                h = await env.client.start_workflow(wf.run, id=str(uuid.uuid4()), task_queue="q")
                if cancel:
                    await h.cancel()
                try:
                    await h.result()
                except Exception:
                    pass
                return list(sent)

            msgs = await outcome(Failing)
            assert len(msgs) == 1 and "Failing" in msgs[0] and "hdtc OOMKilled" in msgs[0], msgs
            assert await outcome(Waits, cancel=True) == [], "cancellation must not alert"
            msgs = await outcome(Parent)
            assert len(msgs) == 1 and "`Failing`" in msgs[0], f"child failure alerted per level: {msgs}"

    assert not should_alert("QLeverIndexWorkflow", ApplicationError("x")), "it self-reports"


if __name__ == "__main__":
    check_webhook()
    asyncio.run(check_workflows())
    print("failure alerts: webhook 404/500 and workflow failures reach Slack once; cancels stay quiet")
