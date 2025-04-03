import logging
from http.client import HTTPException

import uvicorn
from fastapi import FastAPI, BackgroundTasks, Query, Body
from models.lakefs_models import LakefsMergeActionModel, LakefTagCreationModel
from models.kg_metadata import KGConfig
from celery_tasks.celery import create_hdt_conversion_job, create_deployment, create_neo4j_conversion_job
from lakefs_util.io_util import download_files, upload_files, download_hdt_files, open_file_with_retry
from config import config
from canary.mail import mail_canary
from canary.slack import slack_canary


app = FastAPI(title="KACE Server",
              description="Kubernetes artifacts Creation Engine (KACE) is a webserver for "
                          "managing k8s workflows and event in sync with Lakefs data updates.")


@app.post('/upload_hdt_callback')
async def upload_hdt_callback(action_model: LakefsMergeActionModel,
                              converted_hdt: bool = Query(False)):
    kg_config = (await KGConfig.from_git()).get_by_repo(action_model.repository_id)
    hdt_location = config.local_data_dir + '/' + action_model.repository_id + '/' + action_model.branch_id + '/hdt'
    try:
        stream = await open_file_with_retry(
            config.local_data_dir + action_model.repository_id + '/' + action_model.branch_id + '/' + 'pr.md',
            "r"
        )
        with stream:
            pr_link, git_branch = stream.read().split(',')
    except Exception as e:
        slack_canary.notify_event("⚠️ Github documentation PR link not found",
                                  repository_id=action_model.repository_id,
                                  branch_id=action_model.branch_id,
                                  error=e)
        raise e

    try:
        stable_branch = await upload_files(
            repo=action_model.repository_id,
            root_branch=action_model.branch_id,
            # If no conversion is done we will use this as a way to create a branch to tag.
            local_files=[
                (f'{hdt_location}/graph.hdt', 'hdt'),
                (f'{hdt_location}/graph.hdt.index.v1-1', 'hdt'),
            ] if converted_hdt else []
        )

        branch_name = stable_branch["stable_branch_name"]
        tag = stable_branch["future_tag"]
        slack_canary.notify_event("✔️ HDT file uploaded.",
                                  repository_id=action_model.repository_id,
                                  branch_id=action_model.branch_id,
                                  )
    except Exception as e:
        slack_canary.notify_event("⚠️ HDT file uploaded failed.",
                                  repository_id=action_model.repository_id,
                                  branch_id=action_model.branch_id,
                                  error=e)
        raise e
    email_to = kg_config.contact.email
    mail_canary.send_review_email(
        recipient_email=email_to,
        branch_name=branch_name,
        version=tag,
        repository_name=action_model.repository_id,
        github_pr=pr_link,
        github_branch=git_branch
    )
    slack_canary.notify_event("✔️ Conversion and Documentation complete.",
                              repository_id=action_model.repository_id,
                              branch_id=action_model.branch_id,
                              github_pr=pr_link,
                              lakefs_branch=branch_name
                              )


@app.post('/upload_neo4j_files')
async def upload_neo4j_files(action_model: LakefsMergeActionModel):
    neo4j_location = config.local_data_dir + '/' + action_model.repository_id + '/' + action_model.branch_id + '/neo4j-export'
    logs_location = config.local_data_dir + '/' + action_model.repository_id + '/' + action_model.branch_id + '/neo4j-logs'
    try:
        stable_branch_name = await upload_files(
            repo=action_model.repository_id,
            root_branch=action_model.branch_id,
            local_files=[
                (f'{neo4j_location}/neo4j-apoc-export.json', 'neo4j-export'),
                (f'{neo4j_location}/stats.txt', 'neo4j-export'),

            ]
        )
        slack_canary.notify_slack(
            "✔️ Neo4j dump has been converted",
            repository=action_model.repository_id,
            **stable_branch_name
        )
        # @TODO This needs to fire off rdf-hdt conversion once complete.
    except Exception as e :
        slack_canary.notify_event("⚠️ Neo4j rdf file upload failed.",
                                  repository_id=action_model.repository_id,
                                  branch_id=action_model.branch_id,
                                  error=e)


async def create_HDT_conversion_task(
        action_model: LakefsMergeActionModel,
        cpu,
        memory,
        ephemeral,
        java_opts,
        mem_size,
        convert_hdt,
        hdt_path,
        doc_path,
        kg_title,
        compressed
):
    if convert_hdt:
        if not compressed:
            files = await download_files(repo=action_model.repository_id, branch=action_model.branch_id)
        else:
            files = await download_files(repo=action_model.repository_id, branch=action_model.branch_id, extensions=[
                "gz", "bz2"
            ])
    else:
        files = await download_files(repo=action_model.repository_id, branch=action_model.branch_id, extensions=[
            'hdt'
        ])

    create_hdt_conversion_job.delay(action_model.dict(),
                                    files_list=files,
                                    cpu=cpu,
                                    memory=memory,
                                    ephemeral=ephemeral,
                                    java_opts=java_opts,
                                    mem_size=mem_size,
                                    convert_to_hdt=convert_hdt,
                                    hdt_path=hdt_path,
                                    doc_path=doc_path,
                                    kg_title=kg_title
                                    )


@app.post("/convert_to_hdt")
async def convert_to_hdt(action_model: LakefsMergeActionModel,
                         background_tasks: BackgroundTasks,
                         cpu: int = Query(3),
                         memory: str = Query("28Gi"),
                         ephemeral: str = Query("2Gi"),
                         java_opts: str = Query("-Xmx25G -Xms25G -Xss512m -XX:+UseParallelGC"),
                         mem_size: str = Query("25G"),
                         hdt_exists: bool = Query(False),
                         hdt_path: str = Query('hdt/'),
                         compressed: bool = Query(False)
                         ):
    try:
        kg_config = (await KGConfig.from_git()).get_by_repo(repo_id=action_model.repository_id)
    except Exception as e:
        slack_canary.send_slack_message(
            f"⚠️ Failed to read registry config from {config.kg_config_url}, {str(e)}"
        )
        raise e
    slack_canary.notify_event(
        event_name="Merge to Main",
        repository=action_model.repository_id,
        commit=action_model.commit_id,
        branch=action_model.branch_id
    )
    # do validation here for rdf supported extensions.
    background_tasks.add_task(create_HDT_conversion_task,
                              action_model,
                              cpu=cpu,
                              memory=memory,
                              ephemeral=ephemeral,
                              java_opts=java_opts,
                              mem_size=mem_size,
                              convert_hdt=not hdt_exists,
                              hdt_path=hdt_path,
                              doc_path=kg_config.frink_options.documentation_path,
                              kg_title=kg_config.shortname,
                              compressed=compressed
                              )
    return "Started conversion, please check repo tag for uploads."


@app.post("/handle_tag_creation")
async def handle_tag_creation(background_tasks: BackgroundTasks,
                              kg_name: str = Query(None),
                              cpu: str = Query(None),
                              memory: str = Query(None),
                              hdt_path: str = Query('hdt/'),
                              notify_email: str = Query(''),
                              action_model: LakefTagCreationModel=Body(...)
                              ):
    background_tasks.add_task(create_deployment_task, kg_name, cpu, memory, hdt_path, action_model, notify_email)
    slack_canary.notify_event(
        event_name="Tag is created , proceeding to deployment",
        repository=action_model.repository_id,
        tag_created=action_model.tag_id,
        external_spaql_address=f"{config.frink_address}/{kg_name}/sparql"
    )
    return f"Started deployment, anticipated address {config.frink_address}/{kg_name}/sparql"


async def create_deployment_task(kg_name: str, cpu: str, memory: str, hdt_path: str, action_model: LakefTagCreationModel,
                                 notify_email: str):
    await download_hdt_files(repo=action_model.repository_id, branch=action_model.tag_id, kg_name=kg_name,
                             hdt_path=hdt_path)
    create_deployment.delay(kg_name=kg_name, cpu=cpu, memory=memory, lakefs_action=action_model.dict(),
                            notify_email=notify_email)


@app.post("/convert_neo4j_to_hdt")
async def convert_neo4j_to_hdt(action_model: LakefsMergeActionModel, background_tasks: BackgroundTasks):
    # do validation here , to make sure the files needed are present.
    slack_canary.notify_event(
        event_name="New data upload on Neo4j based repo. Conversion starting",
        repository=action_model.repository_id,
        commit=action_model.commit_id,
        branch=action_model.branch_id
    )
    background_tasks.add_task(create_neo4j_HDT_conversion_task, action_model)
    return "Started conversion, please check repo tag for uploads."


async def create_neo4j_HDT_conversion_task(action_model: LakefsMergeActionModel):
    neo4j_files = await download_files(action_model.repository_id, branch=action_model.branch_id, extensions=['dump'])
    create_neo4j_conversion_job.delay(action_model.dict(),
                                      files_list=neo4j_files,
                                      )


@app.post("/validate_tag")
async def validate_tag(action_model: LakefTagCreationModel):
    return {"message": "successfully created tag"}

if __name__ == "__main__":
    uvicorn.run(app, host="0.0.0.0", port=9898)
