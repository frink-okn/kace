from pydantic import BaseModel
import os






class Config(BaseModel):
    lakefs_access_key: str
    lakefs_secret_key: str
    lakefs_url: str
    lakefs_public_url: str
    shared_data_dir: str
    local_data_dir: str
    k8s_namespace: str
    shared_pvc_name: str
    local_pvc_name: str
    hdt_upload_callback_url: str
    neo4j_upload_callback_url: str
    spider_ip: str
    spider_port: int
    frink_address: str
    slack_token: str
    slack_channel: str
    email_address: str
    email_password: str
    smtp_port: int
    smtp_server: str
    gh_token: str
    kg_config_url: str
    stop_email: str
    temporal_host: str
    temporal_namespace: str
    networking_mode: str
    void_repo: str
    qlever_storage_class: str
    qlever_use_private_pvc: bool
    conversion_skip_repos: list[str]
    ldf_pvc_name: str
    ldf_pvc_size: str
    ldf_pvc_storage_class: str
    ldf_replicas: int
    ldf_image: str
    ldf_extra_datasources: list[dict]
    ldf_sync_image: str
    ldf_host_name: str
    qlever_image: str
    qlever_indexer_cpu: str
    qlever_indexer_memory: str
    qlever_indexer_stxxl_memory: str
    qlever_index_pvc_size: str
    qlever_index_pvc_storage_class: str
    qlever_source_path: str
    qlever_state_configmap: str
    qlever_index_pvc_prefix: str
    qlever_index_previous_ttl_hours: int
    qlever_download_concurrency: int
    qlever_num_triples_per_batch: int



config = Config(
    lakefs_access_key=os.environ.get('LAKEFS_ACCESS_KEY',''),
    lakefs_secret_key=os.environ.get('LAKEFS_SECRET_KEY',''),
    lakefs_public_url = os.environ.get('LAKEFS_PUBLIC_URL',''),
    lakefs_url=os.environ.get('LAKEFS_URL','https://frink-lakefs.apps.renci.org'),
    shared_data_dir=os.environ.get('SHARED_DATA_DIR',''),
    local_data_dir=os.environ.get('LOCAL_DATA_DIR',f'{os.path.dirname(__file__)}/../data'),
    k8s_namespace=os.environ.get('K8S_NAMESPACE', ''),
    shared_pvc_name=os.environ.get('SHARED_PVC_NAME', ''),
    local_pvc_name=os.environ.get('LOCAL_PVC_NAME', ''),
    hdt_upload_callback_url=os.environ.get('HDT_UPLOAD_CALLBACK_URL', 'http://localhost:9898/upload_hdt_callback'),
    neo4j_upload_callback_url=os.environ.get('NEO4J_UPLOAD_CALLBACK_URL', 'http://localhost:9898/upload_neo4j_files'),
    slack_webhook_url=os.environ.get('SLACK_URL', ''),
    spider_ip=os.environ.get('SPIDER_IP', ''),
    spider_port=int(os.environ.get('SPIDER_PORT', 9090)),
    frink_address=os.environ.get('FRINK_ADDRESS', 'https://frink.apps.renci.org'),
    slack_token=os.environ.get('SLACK_TOKEN', ''),
    slack_channel=os.environ.get('SLACK_CHANNEL', ''),
    email_address=os.environ.get('EMAIL_ADDRESS', ''),
    email_password=os.environ.get('EMAIL_PASSWORD', ''),
    smtp_server=os.environ.get('SMTP_SERVER', ''),
    smtp_port=int(os.environ.get('SMTP_PORT', 25)),
    gh_token=os.environ.get('GH_TOKEN', ''),
    kg_config_url=os.environ.get('KG_CONFIG_URL', 'https://raw.githubusercontent.com/frink-okn/okn-registry/refs/heads/main/docs/registry/kgs.yaml'),
    stop_email=os.environ.get('STOP_EMAIL', ''),
    temporal_host=os.environ.get('TEMPORAL_HOST', 'localhost:7233'),
    temporal_namespace=os.environ.get('TEMPORAL_NAMESPACE', 'default'),
    networking_mode=os.environ.get('NETWORKING_MODE', 'ingress'),
    void_repo=os.environ.get('VOID_REPO', 'okn-void:develop'),
    qlever_storage_class=os.environ.get('QLEVER_STORAGE_CLASS', ''),
    qlever_use_private_pvc=os.environ.get('QLEVER_USE_PRIVATE_PVC', 'true').lower() == 'true',
    conversion_skip_repos=[r.strip() for r in os.environ.get('CONVERSION_SKIP_REPOS', '').split(',') if r.strip()],
    ldf_pvc_name=os.environ.get('LDF_PVC_NAME', 'frink-ldf-data'),
    ldf_pvc_size=os.environ.get('LDF_PVC_SIZE', '500Gi'),
    ldf_pvc_storage_class=os.environ.get('LDF_PVC_STORAGE_CLASS', ''),
    ldf_replicas=int(os.environ.get('LDF_REPLICAS', '2')),
    ldf_image=os.environ.get('LDF_IMAGE', 'containers.renci.org/frink/ldf-server:2023-09-13'),
    ldf_extra_datasources=__import__('json').loads(os.environ.get('LDF_EXTRA_DATASOURCES', '[]')),
    ldf_sync_image=os.environ.get('LDF_SYNC_IMAGE', ''),
    ldf_host_name=os.environ.get('LDF_HOST_NAME', 'frink.apps.renci.org'),
    qlever_image=os.environ.get('QLEVER_IMAGE', 'adfreiburg/qlever:commit-99b6db5'),
    qlever_indexer_cpu=os.environ.get('QLEVER_INDEXER_CPU', '8'),
    qlever_indexer_memory=os.environ.get('QLEVER_INDEXER_MEMORY', '100Gi'),
    qlever_indexer_stxxl_memory=os.environ.get('QLEVER_INDEXER_STXXL_MEMORY', '40G'),
    qlever_index_pvc_size=os.environ.get('QLEVER_INDEX_PVC_SIZE', '3Ti'),
    qlever_index_pvc_storage_class=os.environ.get('QLEVER_INDEX_PVC_STORAGE_CLASS', 'premium-rwo'),
    qlever_source_path=os.environ.get('QLEVER_SOURCE_PATH', '/shared/qlever-source'),
    qlever_state_configmap=os.environ.get('QLEVER_STATE_CONFIGMAP', 'kace-qlever-state'),
    qlever_index_pvc_prefix=os.environ.get('QLEVER_INDEX_PVC_PREFIX', 'kace-qlever-index-'),
    qlever_index_previous_ttl_hours=int(os.environ.get('QLEVER_INDEX_PREVIOUS_TTL_HOURS', '24')),
    qlever_download_concurrency=int(os.environ.get('QLEVER_DOWNLOAD_CONCURRENCY', '4')),
    qlever_num_triples_per_batch=int(os.environ.get('QLEVER_NUM_TRIPLES_PER_BATCH', '5000000')),
)
