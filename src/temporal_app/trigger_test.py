"""
Quick script to trigger a test workflow execution.
Run from src/: python -m temporal_app.trigger_test

This will start an HDTConversionWorkflow with dummy data so you can
see it appear in the Temporal UI at http://localhost:8080.
The workflow will fail (no real K8s cluster), but it proves the
worker ↔ server ↔ UI pipeline works end-to-end.
"""
import asyncio
from temporalio.client import Client
from config import config


async def main():
    client = await Client.connect(
        config.temporal_host,
        namespace=config.temporal_namespace,
    )

    # Start a test HDTConversionWorkflow
    handle = await client.start_workflow(
        "HDTConversionWorkflow",  # workflow name (string form)
        args=[
            # action_payload
            {
                "hook_id": "test-hook",
                "repository_id": "climatepub4-kg",
                "branch_id": "main",
                "commit_id": "2cff306f9bf8495b6c28cd4776cf799c0dc432015b9f6dbe1c8410e972049ec9",
                "source_ref": "2cff306f9bf8495b6c28cd4776cf799c0dc432015b9f6dbe1c8410e972049ec9",
            },
            'path-1',
            'title'
            # "https://raw.githubusercontent.com/frink-okn/okn-registry/refs/heads/main/docs/registry/neo4j-conf/spoke-genelab.yaml"
        ],
        id="test-hdtc-001",
        task_queue="frink-temporal-queue",
    )

    print(f"✅ Workflow started!")
    print(f"   Workflow ID: {handle.id}")
    print(f"   Run ID:      {handle.result_run_id}")
    print(f"   View it at:  http://localhost:51868/namespaces/default/workflows/{handle.id}")


if __name__ == "__main__":
    asyncio.run(main())
