import os, shutil
import aioboto3
import lakefs.client

import lakefs_sdk.configuration
from aiofile import async_open
from config import config
from log_util import LoggingUtil
from lakefs_util.semver_util import get_latest_version, bump_version
from lakefs.models import Commit


logger = LoggingUtil.init_logging('lakefs-io')


async def download_files(repo: str, branch: str):
    base_dir = config.local_data_dir + '/' + repo
    rdf_files = []
    os.makedirs(base_dir, exist_ok=True)
    clean_up_files(repo=repo)
    endpoint_url = config.lakefs_url
    access_key = config.lakefs_access_key
    secret_key = config.lakefs_secret_key
    logger.info("Local data dir: {}".format(base_dir))

    session = aioboto3.Session()
    async with session.resource(
            's3',
            endpoint_url=endpoint_url,
            aws_access_key_id=access_key,
            aws_secret_access_key=secret_key
    ) as s3_client:
        bucket = await s3_client.Bucket(repo)

        async for obj in bucket.objects.filter(Prefix=branch + '/'):
            logger.info(f"Downloading {obj.key}")

            download_path = os.path.join(base_dir, obj.key)
            rdf_textual_extensions = [
                "rdf",  # RDF/XML
                "xml",  # RDF/XML, TriX
                "ttl",  # Turtle
                "nt",  # N-Triples
                "nq",  # N-Quads
                "jsonld",  # JSON-LD
                "json",  # JSON-LD
                "rj",  # RDF/JSON
                "trig",  # TriG
                "trix",  # TriX
                "n3"  # N3
            ]
            if obj.key.split('.')[-1] in rdf_textual_extensions:
                rdf_files.append(obj.key.lstrip(branch).lstrip('/'))
                os.makedirs(os.path.dirname(download_path), exist_ok=True)
                stream_body = (await obj.get())['Body']
                async with async_open(download_path, 'wb') as out_file:
                    while file_data := await stream_body.read():
                        await out_file.write(file_data)
                logger.info(f"Downloaded {obj.key} to {download_path}")
    return rdf_files


async def download_hdt_files(repo: str, branch: str, kg_name: str, hdt_path: str='/hdt/') -> None:
    base_dir = config.shared_data_dir + '/deploy'
    # @TODO download into a temp name then rename
    os.makedirs(base_dir, exist_ok=True)
    endpoint_url = config.lakefs_url
    access_key = config.lakefs_access_key
    secret_key = config.lakefs_secret_key
    logger.info("Local data dir: {}".format(base_dir))
    session = aioboto3.Session()
    async with session.resource(
            's3',
            endpoint_url=endpoint_url,
            aws_access_key_id=access_key,
            aws_secret_access_key=secret_key
    ) as s3_client:
        bucket = await s3_client.Bucket(repo)
        renames = {}
        async for obj in bucket.objects.filter(Prefix=branch + hdt_path):
            if obj.key.endswith('.hdt') or obj.key.endswith('.hdt.index.v1-1'):
                logger.info(f"Downloading {obj.key}")
                temp_file_name = branch + '-' + kg_name + "." + ".".join(obj.key.split('/')[-1].split('.')[1:])
                download_path = os.path.join(base_dir, temp_file_name)
                final_file_path = os.path.join(base_dir, kg_name + "." + ".".join(obj.key.split('/')[-1].split('.')[1:]))
                renames[download_path] = final_file_path
            os.makedirs(os.path.dirname(download_path), exist_ok=True)
            stream_body = (await obj.get())['Body']
            async with async_open(download_path, 'wb') as out_file:
                while file_data := await stream_body.read():
                   await out_file.write(file_data)
            logger.info(f"Downloaded {obj.key} to {download_path}")
        logger.info("Moving files")
        for temp_file_name, file_name in renames.items():
            os.rename(temp_file_name, file_name)
            logger.info(f"Moved {temp_file_name} -> {file_name}")


async def upload_hdt_files(repo: str, root_branch: str = "main", local_files: list[str] = None, remote_path=""):
    """Upload the result and clear dir"""
    client = lakefs.client.LakeFSClient(configuration=lakefs_sdk.configuration.Configuration(
        config.lakefs_url,username=config.lakefs_access_key, password=config.lakefs_secret_key
    ))
    max = 0
    latest_tag = ""
    results = client.tags_api.list_tags(repo)
    pagination = results.pagination
    tags = results.results
    while pagination.has_more:
        results = client.tags_api.list_tags(repo,after=pagination.next_offset)
        pagination = results.pagination
        tags += results

    versions = [tag.id.lstrip('v') for tag in tags]
    latest_tag =  "v" + bump_version(get_latest_version(versions), "patch") if len(versions) else "v0.0.1"
    stable_branch_name = f"stable_{latest_tag.replace('.', '_')}"
    try:
        client.branches_api.create_branch(repository=repo, branch_creation={
            "name": stable_branch_name,
            "source": root_branch
        })
    except Exception as e:
        logger.error(e)
        pass
    for file in local_files:
        with open(file, "rb") as stream:
            file_content = stream.read()
            content = bytearray(file_content)
            path = remote_path + '/' + os.path.basename(file)
            client.objects_api.upload_object(repository=repo,
                                            branch=stable_branch_name,
                                            path=path,
                                            content=content)

    # Commit
    client.commits_api.commit(repository=repo, branch=stable_branch_name, commit_creation={
        "message": f"HDT Uploads for version {latest_tag}",
        "metadata": {
            "key": "value"
        }
    })

    # create tag
    client.tags_api.create_tag(repository=repo, tag_creation={
        "id": latest_tag,
        "ref": stable_branch_name
    })



def clean_up_files(repo: str):
    base_dir = config.local_data_dir + '/' + repo
    for filename in os.listdir(base_dir):
        file_path = os.path.join(base_dir, filename)
        try:
            if os.path.isfile(file_path) or os.path.islink(file_path):
                os.unlink(file_path)
            elif os.path.isdir(file_path):
                shutil.rmtree(file_path)
        except Exception as e:
            print('Failed to delete %s. Reason: %s' % (file_path, e))


def resolve_commit(repo, commit_id) -> Commit:
    client = lakefs.client.LakeFSClient(configuration=lakefs_sdk.configuration.Configuration(
        config.lakefs_url, username=config.lakefs_access_key, password=config.lakefs_secret_key
    ))
    return client.commits_api.get_commit(repository=repo, commit_id=commit_id)




if __name__ == '__main__':
    import asyncio
    # asyncio.run(download_hdt_files('climatepub4-kg', 'v0.0.2', 'climatekg' ))
    result = resolve_commit('test-hook-repo', '971c4460e367bb9c34e112a5e49b704d5400e9b9fde878b9f927e5c1fb177e4b')
    print(result)