from http.client import HTTPException

import uvicorn
from fastapi import FastAPI, BackgroundTasks, Query, Body
from models.lakefs_models import LakefsMergeActionModel, LakefTagCreationModel
from celery_tasks.celery import create_hdt_conversion_job, create_deployment, create_neo4j_conversion_job
from lakefs_util.io_util import download_files, upload_files, download_hdt_files
from config import config
from canary.mail import mail_canary
from canary.slack import slack_canary



app = FastAPI(title="KACE Server",
              description="Kubernetes artifacts Creation Engine (KACE) is a webserver for "
                          "managing k8s workflows and event in sync with Lakefs data updates.")


@app.post('/upload_hdt_callback')
async def upload_hdt_callback(action_model: LakefsMergeActionModel, notify_email: str = Query('')):
    hdt_location = config.local_data_dir + '/' + action_model.repository_id + '/' + action_model.branch_id + '/hdt'
    report_location = config.local_data_dir + '/' + action_model.repository_id + '/' + action_model.branch_id + '/report'
    try:
        stable_branch = await upload_files(
            repo=action_model.repository_id,
            root_branch=action_model.branch_id,
            local_files=[
                (f'{hdt_location}/graph.hdt', 'hdt'),
                (f'{hdt_location}/graph.hdt.index.v1-1', 'hdt'),
                # (f'{report_location}/graph_stats.json', 'report'),
                # (f'{report_location}/riot_validate.log', 'report'),
                # (f'{report_location}/schema.dot', 'report')
            ]
        )
        email_to = notify_email
        branch_name = stable_branch["stable_branch_name"]
        tag = stable_branch["future_tag"]

        mail_canary.send_review_email(
            recipient_email=email_to,
            branch_name=branch_name,
            version=tag,
            repository_name=action_model.repository_id
        )
        slack_canary.notify_event("✔️ HDT file uploaded.",
                                  repository_id=action_model.repository_id,
                                  branch_id=action_model.branch_id,
                                  )
    except Exception as e :
        slack_canary.notify_event("⚠️ HDT file uploaded failed.",
                                  repository_id=action_model.repository_id,
                                  branch_id=action_model.branch_id,
                                  error=e)



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
        notify_email
):
    rdf_files = await download_files(repo=action_model.repository_id, branch=action_model.branch_id)
    create_hdt_conversion_job.delay(action_model.dict(),
                                    files_list=rdf_files,
                                    cpu=cpu,
                                    memory=memory,
                                    ephemeral=ephemeral,
                                    java_opts=java_opts,
                                    mem_size=mem_size,
                                    notify_email=notify_email
                                    )


@app.post("/convert_to_hdt")
async def convert_to_hdt(action_model: LakefsMergeActionModel,
                         background_tasks: BackgroundTasks,
                         cpu=Query(3),
                         memory=Query("28Gi"),
                         ephemeral=Query("2Gi"),
                         java_opts=Query("-Xmx25G -Xms25G -Xss512m -XX:+UseParallelGC"),
                         mem_size=Query("25G"),
                         notify_email=Query('')
                         ):
    slack_canary.notify_event(
        event_name="Converting files to HDT",
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
                              notify_email=notify_email
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
