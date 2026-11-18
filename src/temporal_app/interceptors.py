"""Slack alert whenever any workflow fails.

Most workflows never reported their own failures, so a broken conversion or
deployment surfaced only when a KG owner emailed to ask where their endpoint
was. This wraps every workflow the worker runs, so new workflows are covered
without remembering to add a try/except.

Not alerted:
  - cancellations: the webhooks cancel in-flight runs on purpose on every re-fire;
  - ChildWorkflowError: the child (e.g. HDT under Neo4j) already alerted;
  - SELF_REPORTING workflows, which post their own richer failure message.

Only FailureError is caught. Any other exception fails the workflow *task*, not
the workflow: Temporal retries the task until the code is fixed, and commands
issued from that task (like this alert) are discarded anyway.
"""
from datetime import timedelta

from temporalio import workflow
from temporalio.common import RetryPolicy
from temporalio.exceptions import ChildWorkflowError, FailureError, is_cancelled_exception
from temporalio.worker import (
    ExecuteWorkflowInput,
    Interceptor,
    WorkflowInboundInterceptor,
    WorkflowInterceptorClassInput,
)

SELF_REPORTING = {"QLeverIndexWorkflow"}


def should_alert(workflow_type: str, err: BaseException) -> bool:
    return not (
        workflow_type in SELF_REPORTING
        or isinstance(err, ChildWorkflowError)
        or is_cancelled_exception(err)
    )


def failure_message(workflow_type: str, workflow_id: str, err: BaseException) -> str:
    # "Activity task failed" says nothing; the useful text is at the bottom of
    # the cause chain (the activity's own exception).
    while getattr(err, "cause", None) is not None:
        err = err.cause
    return f"❌ `{workflow_type}` failed (`{workflow_id}`): {type(err).__name__}: {str(err)[:1500]}"


class _AlertOnFailure(WorkflowInboundInterceptor):
    async def execute_workflow(self, input: ExecuteWorkflowInput):
        try:
            return await super().execute_workflow(input)
        except FailureError as err:
            info = workflow.info()
            if should_alert(info.workflow_type, err):
                try:
                    # By name: the activity module is not importable in the sandbox.
                    await workflow.execute_activity(
                        "notify_slack",
                        failure_message(info.workflow_type, info.workflow_id, err),
                        start_to_close_timeout=timedelta(minutes=1),
                        retry_policy=RetryPolicy(maximum_attempts=3),
                    )
                except Exception:
                    pass  # the alert must never replace the real failure
            raise


class SlackFailureInterceptor(Interceptor):
    def workflow_interceptor_class(self, input: WorkflowInterceptorClassInput):
        return _AlertOnFailure
