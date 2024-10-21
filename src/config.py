from pydantic import BaseModel
import os



class Config(BaseModel):
    lakefs_access_key: str
    lakefs_secret_key: str
    lakefs_url: str
    shared_data_dir: str
    local_data_dir: str
    k8s_namespace: str
    shared_pvc_name: str
    local_pvc_name: str
    hdt_upload_callback_url: str
    slack_webhook_url: str
    spider_ip: str
    spider_port: int
    frink_address: str


config = Config(
    lakefs_access_key=os.environ.get('LAKEFS_ACCESS_KEY',''),
    lakefs_secret_key=os.environ.get('LAKEFS_SECRET_KEY',''),
    lakefs_url=os.environ.get('LAKEFS_URL','https://frink-lakefs.apps.renci.org'),
    shared_data_dir=os.environ.get('SHARED_DATA_DIR',''),
    local_data_dir=os.environ.get('LOCAL_DATA_DIR',f'{os.path.dirname(__file__)}/../data'),
    k8s_namespace=os.environ.get('K8S_NAMESPACE', ''),
    shared_pvc_name=os.environ.get('SHARED_PVC_NAME', ''),
    local_pvc_name=os.environ.get('LOCAL_PVC_NAME', ''),
    hdt_upload_callback_url=os.environ.get('HDT_UPLOAD_CALLBACK_URL', 'http://localhost:9898/upload_hdt_callback'),
    slack_webhook_url=os.environ.get('SLACK_URL', ''),
    spider_ip=os.environ.get('SPIDER_IP', ''),
    spider_port=int(os.environ.get('SPIDER_PORT', '')),
    frink_address=os.environ.get('FRINK_ADDRESS', 'frink.apps.renci.org')

)
