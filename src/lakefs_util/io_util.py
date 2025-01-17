import os, shutil
import lakefs.client
import aiohttp
import lakefs_sdk.configuration

from config import config
from log_util import LoggingUtil
from lakefs_util.semver_util import get_latest_version, bump_version
from lakefs.models import Commit
from typing import Union, List
from lakefs_util.lakefs_login import login_and_get_cookies

import urllib.parse


logger = LoggingUtil.init_logging('lakefs-io')


def clear_directory(path, delete_root=False):
    """
    Recursively clears a directory by deleting all files and subdirectories within it.
    Optionally deletes the supplied directory itself.

    :param path: The path to the directory to be cleared.
    :param delete_root: If True, the supplied directory will also be deleted. Default is False.
    """
    # Check if the path exists
    if not os.path.exists(path):
        raise FileNotFoundError(f"The directory '{path}' does not exist.")

    # Check if the path is a directory
    if not os.path.isdir(path):
        raise NotADirectoryError(f"'{path}' is not a directory.")

    # Iterate over the contents of the directory
    for item in os.listdir(path):
        item_path = os.path.join(path, item)

        try:
            # If the item is a file or a symbolic link, delete it
            if os.path.isfile(item_path) or os.path.islink(item_path):
                os.unlink(item_path)
                print(f"Deleted file: {item_path}")

            # If the item is a directory, recursively clear it and then delete it
            elif os.path.isdir(item_path):
                clear_directory(item_path, delete_root=True)  # Recursively clear the subdirectory
                shutil.rmtree(item_path)  # Delete the now-empty directory
                print(f"Deleted directory: {item_path}")

        except FileNotFoundError:
            print(f"Warning: The path '{item_path}' no longer exists. Skipping...")
        except PermissionError:
            print(f"Warning: Permission denied for '{item_path}'. Skipping...")
        except Exception as e:
            print(f"Warning: An error occurred while processing '{item_path}': {e}. Skipping...")

    # Optionally delete the root directory
    if delete_root:
        try:
            shutil.rmtree(path)
            print(f"Deleted root directory: {path}")
        except FileNotFoundError:
            print(f"Warning: The root directory '{path}' no longer exists. Skipping...")
        except PermissionError:
            print(f"Warning: Permission denied to delete the root directory '{path}'. Skipping...")
        except Exception as e:
            print(f"Warning: An error occurred while deleting the root directory '{path}': {e}. Skipping...")


async def download_files(repo: str, branch: str, extensions: List = None):
    if extensions is None:
        extensions = [
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
    cookie = await login_and_get_cookies(config.lakefs_url, config.lakefs_access_key, config.lakefs_secret_key)
    all_files = []
    files_downloaded = []
    async with aiohttp.ClientSession(cookies=cookie) as session:
        has_more = True
        offset = ""
        while has_more:
            url = lambda offset: (f'{config.lakefs_url}/api/v1/repositories/{urllib.parse.quote_plus(repo)}/refs/'
                                  f'{urllib.parse.quote_plus(branch)}'
                                  f'/objects/ls?after={offset}&amount=1000')
            response = await session.get(url(offset))
            if response.status != 200:
                logger.error(f"Error getting file list")
                raise Exception(f"Error getting file")

            results = await response.json()
            has_more = results["pagination"]["has_more"]
            offset += results["pagination"]["next_offset"]
            all_files += list([x['path'] for x in results["results"]])
        base_dir = config.local_data_dir + '/' + repo + '/' + branch
        # clear dir
        clear_directory(base_dir, delete_root=False)
        for file_name in all_files:
            if file_name.split('.')[-1] in extensions:
                files_downloaded.append(file_name.lstrip('/'))
                download_path = os.path.join(base_dir, file_name)
                await download_file(file_name, repo, branch, download_path, session)
                logger.info(f"Download {file_name} complete")
    return files_downloaded

async def download_file(file_name, repo, branch, download_path,
                         session: aiohttp.ClientSession):
    # get file stats
    stats_endpoint = (f'{config.lakefs_url}/api/v1/repositories/{urllib.parse.quote_plus(repo)}/refs/'
                      f'{urllib.parse.quote_plus(branch)}'
                      f'/objects/stat?path={file_name}')
    response = await session.get(stats_endpoint)
    stat_obj = await response.json()
    file_size = stat_obj["size_bytes"]
    logger.info(f"Downloading {file_name}: {file_size}")
    download_dir = os.path.dirname(download_path)
    if not os.path.exists(download_dir):
        os.makedirs(download_dir)
    with open(download_path, 'wb') as stream:
        file_url = (f'{config.lakefs_url}/api/v1/repositories/{urllib.parse.quote_plus(repo)}/refs/'
                    f'{urllib.parse.quote_plus(branch)}'
                    f'/objects?path={file_name}')
        chunk = 65_536
        current_pos = 0
        while current_pos < file_size:
            from_bytes = current_pos
            to_bytes = min(current_pos + chunk, file_size - 1)
            data = await session.get(file_url, headers={'Range': f'bytes={from_bytes}-{to_bytes}'})
            async for content in data.content:
                stream.write(content)
            current_pos = to_bytes + 1
    logger.info(f"Download {file_name} complete")


async def download_hdt_files(repo: str, branch: str, kg_name: str, hdt_path: str='hdt') -> None:
    base_dir = config.shared_data_dir + '/deploy'
    # @TODO download into a temp name then rename
    cookie = await login_and_get_cookies(config.lakefs_url, config.lakefs_access_key, config.lakefs_secret_key)
    all_files = []
    async with aiohttp.ClientSession(cookies=cookie) as session:
        has_more = True
        offset = ""
        while has_more:
            url = lambda offset: (f'{config.lakefs_url}/api/v1/repositories/{urllib.parse.quote_plus(repo)}/refs/'
                                  f'{urllib.parse.quote_plus(branch)}'
                                  f'/objects/ls?after={offset}&amount=1000?&prefix={hdt_path}') if offset else (
                                    f'{config.lakefs_url}/api/v1/repositories/{urllib.parse.quote_plus(repo)}/refs/'
                                    f'{urllib.parse.quote_plus(branch)}'
                                    f'/objects/ls?amount=1000&prefix={hdt_path}')
            logger.info(url(offset))
            response = await session.get(url(offset))
            if response.status != 200:
                logger.error(f"Error getting file list")
                raise Exception(f"Error getting file")

            results = await response.json()
            has_more = results["pagination"]["has_more"]
            offset += results["pagination"]["next_offset"]
            all_files += list([x['path'] for x in results["results"]])
        renames = {}
        logger.info(f"Files to download: {all_files}")
        for file_name in all_files:
            if file_name.endswith('.hdt') or file_name.endswith('.hdt.index.v1-1'):
                temp_file_name = branch + '-' + kg_name + "." + ".".join(file_name.split('/')[-1].split('.')[1:])
                download_path = os.path.join(base_dir, temp_file_name)
                final_file_path = os.path.join(base_dir,
                                               kg_name + "." + ".".join(file_name.split('/')[-1].split('.')[1:]))
                renames[download_path] = final_file_path
                os.makedirs(os.path.dirname(download_path), exist_ok=True)
                await download_file(
                    file_name, repo, branch, download_path, session
                )
        for temp_file_name, file_name in renames.items():
            os.rename(temp_file_name, file_name)
            logger.info(f"Moved {temp_file_name} -> {file_name}")


async def upload_files(repo: str, root_branch: str = "main", local_files: list[tuple[str, str]] = None):
    """Upload the result and clear dir"""
    # create client
    client = lakefs.client.LakeFSClient(
        configuration=lakefs_sdk.configuration.Configuration(
            config.lakefs_url,
            username=config.lakefs_access_key,
            password=config.lakefs_secret_key
        )
    )
    # get all tags
    results = client.tags_api.list_tags(repo)
    pagination = results.pagination
    tags = results.results
    while pagination.has_more:
        results = client.tags_api.list_tags(repo,after=pagination.next_offset)
        pagination = results.pagination
        tags += results
    # compute latest tag
    versions = [tag.id.lstrip('v') for tag in tags]
    latest_tag = "v" + bump_version(get_latest_version(versions), "patch") if len(versions) else "v0.0.1"
    stable_branch_name = f"stable_{latest_tag.replace('.', '_')}"
    # create branch if not exists
    try:
        client.branches_api.create_branch(repository=repo, branch_creation={
            "name": stable_branch_name,
            "source": root_branch
        })
    except Exception as e:
        logger.warn(e)

    # push local files.
    login_cookie = await login_and_get_cookies(config.lakefs_url, config.lakefs_access_key, config.lakefs_secret_key)

    for file, remote_path in local_files:
        async with aiohttp.ClientSession(cookies=login_cookie) as session:
            with open(file, "rb") as stream:
                # chunk generator
                async def file_chunks():
                    while True:
                        chunk = stream.read(1024 * 1024)  # 1 MB chunk size
                        if not chunk:
                            break
                        yield chunk

                path = remote_path + '/' + os.path.basename(file)

                url = (f'{config.lakefs_url}/api/v1/repositories/{urllib.parse.quote_plus(repo)}/branches/'
                       f'{urllib.parse.quote_plus(stable_branch_name)}'
                       f'/objects?path={urllib.parse.quote_plus(path)}')
                logger.info(url)

                async with session.post(url, data=file_chunks()) as response:
                    if response.status not in [200, 201]:
                        txt = await response.text()
                        logger.error(f"Error uploading file: {txt}")
                        raise Exception(f"Error uploading file: {response.status}")
                    logger.info(f"Uploaded {path}")

    # Commit
    client.commits_api.commit(repository=repo, branch=stable_branch_name, commit_creation={
        "message": f"Uploads for version {latest_tag}",
        "metadata": {
            "key": "value"
        }
    })

    # create tag
    # client.tags_api.create_tag(repository=repo, tag_creation={
    #     "id": latest_tag,
    #     "ref": stable_branch_name
    # })

    return {
        "stable_branch_name": stable_branch_name,
        "future_tag": latest_tag,
    }


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


# if __name__ == '__main__':
    # import asyncio
    # asyncio.run(
    #     download_hdt_files("climatepub4-kg", 'v0.0.4', kg_name='climates' )
    # )
    #      upload_files("test-hook-repo", root_branch= "main", local_files=[('/home/kebedey/projects/frink/kace/README.MD', 'test_file')]))
