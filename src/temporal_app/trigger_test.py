
import asyncio
from temporalio.client import Client
from config import config
from models.kg_metadata import KGConfig
from models.lakefs_models import LakefTagCreationModel

async def main():
    client = await Client.connect(
        config.temporal_host,
        namespace=config.temporal_namespace,
    )

    # Start a test HDTConversionWorkflow
    # handle = await client.start_workflow(
    #     "HDTConversionWorkflow",  # workflow name (string form)
    #     args=[
    #         # action_payload
    #         {
    #             "hook_id": "test-hook",
    #             "repository_id": "climatepub4-kg",
    #             "branch_id": "main",
    #             "commit_id": "2cff306f9bf8495b6c28cd4776cf799c0dc432015b9f6dbe1c8410e972049ec9",
    #             "source_ref": "2cff306f9bf8495b6c28cd4776cf799c0dc432015b9f6dbe1c8410e972049ec9",
    #         },
    #         'path-1',
    #         # "https://raw.githubusercontent.com/frink-okn/okn-registry/refs/heads/main/docs/registry/neo4j-conf/spoke-genelab.yaml"
    #     ],
    #     id="test-hdtc-001",
    #     task_queue="frink-temporal-queue",
    # )

    # start test deployment

    # LakefTagCreationModel(**  )

    kg= KGConfig.from_git_sync().get_by_repo(repo_id="climatepub4-kg")
    handle = await client.start_workflow(
        "QLeverDeploymentWorkflow",  # workflow name (string form)
        args=[
            # kg_config
            kg,
            # action_payload
            "1",
            "3Gi",
            {"event_type": "post-create-tag",
             "event_time": "2026-03-03T19:51:51Z",
             "action_name": "Deploy sparql endpoints",
             "hook_id": "deploy-update-servers",
             "repository_id": "climatepub4-kg",
             "source_ref": "87db5ae06880c6cbfa24a11ec2b0b29dc29fe01da2b066fa57a55ef364a0b158",
             "tag_id": "v0.0.1",
             "commit_id": "87db5ae06880c6cbfa24a11ec2b0b29dc29fe01da2b066fa57a55ef364a0b158"}
            ,
        ],
        id="test-qlever-deployment-001",
        task_queue="frink-temporal-queue",
    )


    print(f"✅ Workflow started!")
    print(f"   Workflow ID: {handle.id}")
    print(f"   Run ID:      {handle.result_run_id}")
    print(f"   View it at:  http://localhost:51868/namespaces/default/workflows/{handle.id}")


if __name__ == "__main__":
    asyncio.run(main())
