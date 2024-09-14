import uvicorn
from fastapi import FastAPI, BackgroundTasks, Query, Body
from models.lakefs_models import LakefsMergeActionModel, LakefTagCreationModel
from celery_tasks.celery import create_hdt_conversion_job, create_deployment
from lakefs_util.io_util import download_files, upload_hdt_files, download_hdt_files
from config import config



app = FastAPI(title="KACE Server", description="Kubernetes artifacts Creation Engine (KACE) is a webserver for managing k8s workflows and event in sync with Lakefs data updates.")


@app.post('/upload_hdt_callback')
async def upload_hdt_callback(action_model: LakefsMergeActionModel):
    hdt_location = config.local_data_dir + '/' + action_model.repository_id + '/' + action_model.branch_id + '/hdt'
    try:
        await upload_hdt_files(
            repo=action_model.repository_id,
            root_branch=action_model.branch_id,
            local_files=[
                f'{hdt_location}/graph.hdt',
                f'{hdt_location}/graph.hdt.index.v1-1'
            ],
            remote_path="hdt"
        )
        # @TODO add slack notification here (?)

    except Exception as e:
        pass


async def create_HDT_conversion_task(action_model: LakefsMergeActionModel):
    rdf_files = await download_files(repo=action_model.repository_id, branch=action_model.branch_id)
    create_hdt_conversion_job.delay(action_model.dict(), files_list=rdf_files)


@app.post("/convert_to_hdt")
async def convert_to_hdt(action_model: LakefsMergeActionModel, background_tasks: BackgroundTasks):
    background_tasks.add_task(create_HDT_conversion_task, action_model)
    return "Started conversion, please check repo tag for uploads."


@app.post("/handle_tag_creation")
async def handle_tag_creation(background_tasks: BackgroundTasks,
                              kg_name: str = Query(None),
                              cpu: int = Query(None),
                              memory: str = Query(None),
                              action_model: LakefTagCreationModel=Body(...)
                              ):
    background_tasks.add_task(create_deployment_task, kg_name, cpu, memory, action_model)
    return f"Started deployment, anticipated address https://frink.apps.renci.org/{kg_name}/sparql"


async def create_deployment_task(kg_name: str, cpu: int, memory: str, action_model: LakefTagCreationModel):
    await download_hdt_files(repo=action_model.repository_id, branch=action_model.tag_id, kg_name=kg_name)
    create_deployment.delay(kg_name=kg_name, cpu=cpu, memory=memory, lakefs_action=action_model.dict())

if __name__ == "__main__":
    uvicorn.run(app, host="0.0.0.0", port=9898)
