import logging
from http.client import HTTPException

import lakefs
import uvicorn
from fastapi import FastAPI, BackgroundTasks, Query, Body
from models.lakefs_models import LakefsMergeActionModel, LakefTagCreationModel
from models.kg_metadata import KGConfig, KG
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
    nt_location = config.local_data_dir + '/' + action_model.repository_id + '/' + action_model.branch_id + '/nt'
    pr_link, git_branch = "", ""
    #
    # try:
    #     stream = await open_file_with_retry(
    #         config.local_data_dir + action_model.repository_id + '/' + action_model.branch_id + '/' + 'pr.md',
    #         "r"
    #     )
    #     with stream:
    #         pr_link, git_branch = stream.read().split(',')
    # except Exception as e:
    #     slack_canary.notify_event("⚠️ Github documentation PR link not found, moving along with uploading conversion",
    #                               repository_id=action_model.repository_id,
    #                               branch_id=action_model.branch_id,
    #                               error=e)


    try:
        stable_branch = await upload_files(
            repo=action_model.repository_id,
            root_branch=action_model.branch_id,
            # If no conversion is done we will use this as a way to create a branch to tag.
            local_files=[
                (f'{hdt_location}/graph.hdt', 'hdt'),
                (f'{hdt_location}/graph.hdt.index.v1-1', 'hdt'),
                (f'{nt_location}/graph.nt.gz', 'nt'),
            ] if converted_hdt else []
        )

        branch_name = stable_branch["stable_branch_name"]
        tag = stable_branch["future_tag"]
        slack_canary.notify_event("✔️ HDT & NT file uploaded.",
                                  repository_id=action_model.repository_id,
                                  branch_id=action_model.branch_id,
                                  )
    except Exception as e:
        slack_canary.notify_event("⚠️ HDT & NT file upload failed.",
                                  repository_id=action_model.repository_id,
                                  branch_id=action_model.branch_id,
                                  error=e)
        raise e
    if kg_config.emails:
        email_to = ",".join(kg_config.emails)
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
async def upload_neo4j_files(action_model: LakefsMergeActionModel, background_tasks: BackgroundTasks):
    neo4j_location = config.local_data_dir + '/' + action_model.repository_id + '/' + action_model.branch_id + '/neo4j-export'
    nt_location = config.local_data_dir + '/' + action_model.repository_id + '/' + action_model.branch_id + '/nt/graph.nt.gz'
    try:
        stable_branch_name = await upload_files(
            repo=action_model.repository_id,
            root_branch=action_model.branch_id,
            local_files=[
                (nt_location, 'nt')

            ]
        )
        slack_canary.notify_event(
            "✔️ Neo4j dump has been converted",
            repository=action_model.repository_id,
            **stable_branch_name
        )
        try:
            kg_config = (await KGConfig.from_git()).get_by_repo(repo_id=action_model.repository_id)
        except Exception as e:
            slack_canary.send_slack_message(
                f"⚠️ Failed to read registry config from {config.kg_config_url}, {str(e)}"
            )
            raise e
        action_model.branch_id = stable_branch_name['stable_branch_name']
        background_tasks.add_task(create_HDT_conversion_task,
                                  action_model,
                                  doc_path=kg_config.frink_options.documentation_path,
                                  kg_title=kg_config.shortname,
                                  cpu=3,
                                  memory="28Gi",
                                  ephemeral="2Gi",
                                  java_opts="-Xmx25G -Xms25G -Xss512m -XX:+UseParallelGC",
                                  mem_size="25G",
                                  convert_hdt=True,
                                  hdt_path="/",
                                  exclude_known_extension=["json"],
                                  exclude_files=None)

    except Exception as e:
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
        exclude_files=None,
        exclude_known_extension=None
):
    if convert_hdt:
        files = await download_files(repo=action_model.repository_id, branch=action_model.branch_id,
                                     exclude_files=exclude_files, exclude_known_extension=exclude_known_extension)
    else:
        files = await download_files(repo=action_model.repository_id, branch=action_model.branch_id, extensions=[
            'hdt'
        ], exclude_files=exclude_files, exclude_known_extension=exclude_known_extension)

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
                         compressed: bool = Query(False),
                         exclude_files: str=Query("")
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
                              exclude_files=[x.strip() for x in exclude_files.split(",")]
                              )
    return "Started conversion, please check repo tag for uploads."


@app.post("/handle_tag_creation")
async def handle_tag_creation(background_tasks: BackgroundTasks,
                              kg_name: str = Query(None),
                              cpu: str = Query(None),
                              memory: str = Query(None),
                              hdt_path: str = Query('hdt/'),
                              action_model: LakefTagCreationModel=Body(...)
                              ):
    kg_config = (await KGConfig.from_git()).get_by_repo(action_model.repository_id)
    kg_name = kg_config.shortname
    background_tasks.add_task(create_deployment_task, cpu, memory, hdt_path, action_model, kg_config)
    slack_canary.notify_event(
        event_name="Tag is created , proceeding to deployment",
        repository=action_model.repository_id,
        tag_created=action_model.tag_id,
        external_spaql_address=f"{config.frink_address}/{kg_name}/sparql"
    )
    return f"Started deployment, anticipated address {config.frink_address}/{kg_name}/sparql"


async def create_deployment_task(cpu: str, memory: str, hdt_path: str, action_model: LakefTagCreationModel,
                                 kg_config: KG):
    await download_hdt_files(repo=action_model.repository_id, branch=action_model.tag_id, kg_name=kg_config.shortname,
                             hdt_path=hdt_path)
    create_deployment.delay(kg_config=kg_config.dict(), cpu=cpu, memory=memory, lakefs_action=action_model.dict())


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
    neo4j_dump_files = await download_files(
        action_model.repository_id,
        branch=action_model.branch_id,
        extensions=['dump']
    )
    neo4j_json_files = await download_files(
        action_model.repository_id,
        branch=action_model.branch_id,
        extensions=['json', 'json.gz'],
        delete_all_files=False
    )

    logging.info(
        f"DUMP Files {neo4j_dump_files}"
    )
    logging.info(
        f"JSON dump files {neo4j_dump_files}"
    )

    try:
        kg_config = (await KGConfig.from_git()).get_by_repo(repo_id=action_model.repository_id)
    except Exception as e:
        slack_canary.send_message(
            f"⚠️ Failed to read registry config from {config.kg_config_url}, {str(e)}"
        )
        raise e
    if len(neo4j_json_files) > 1:
        slack_canary.send_message(
            f"⚠ Error on {action_model.repository_id}, trying to convert neo4j export, repository contains multiple json exports."
        )

    create_neo4j_conversion_job.delay(action_model.dict(),
                                      dump_files_list=neo4j_dump_files,
                                      json_dump_files_list=neo4j_json_files,
                                      rdf_mapping_config=kg_config.frink_options.neo4j_conversion_config_path
                                      )


@app.post("/validate_tag")
async def validate_tag(action_model: LakefTagCreationModel):
    return {"message": "successfully created tag"}

if __name__ == "__main__":
    uvicorn.run(app, host="0.0.0.0", port=9898)
