import json
import os, shutil
import lakefs.client
import aiohttp
import lakefs_sdk.configuration
from lakefs_sdk.exceptions import BadRequestException
import asyncio
from config import config
from log_util import LoggingUtil
from lakefs_util.semver_util import get_latest_version, bump_version
from lakefs.models import Commit
from typing import Union, List, Optional
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


async def download_files(repo: str, branch: str, extensions: List = None, exclude_files: List = None,
                         exclude_known_extension: List = None, delete_all_files=True,
                         exclude_prefixes: List = None):
    rdf_extension = [
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
            "n3",  # N3
            "hdt",
        ]
    compression_extensions = [
        "gz", "bz2"
    ]
    if exclude_files is None:
        exclude_files = []
    if extensions is None:
        extensions = rdf_extension.copy()
        for ext in compression_extensions:
            extensions += [f"{x}.{ext}" for x in rdf_extension]
        extensions += compression_extensions
    if exclude_known_extension:
        extensions = [ext for ext in extensions if ext not in exclude_known_extension]
    logger.info(f"Downloading {extensions} file types from {repo}@{branch} ")
    cookie = await login_and_get_cookies(config.lakefs_url, config.lakefs_access_key, config.lakefs_secret_key)
    all_files = []
    files_downloaded = []
    connector = aiohttp.TCPConnector(limit_per_host=8)
    timeout = aiohttp.ClientTimeout(total=None, sock_read=600, sock_connect=60)
    async with aiohttp.ClientSession(cookies=cookie, connector=connector, timeout=timeout) as session:
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
            offset = results["pagination"]["next_offset"]
            all_files += list([x['path'] for x in results["results"]])
        base_dir = os.path.join(config.local_data_dir, repo, branch)
        if os.path.exists(base_dir):
            if  delete_all_files:
                clear_directory(base_dir, delete_root=False)
        else:
            logger.info(f"Directory {base_dir} does not exist; creating it ")
            os.makedirs(base_dir)
        for file_name in all_files:
            if file_name in exclude_files:
                logger.info(f"Skipping {file_name}")
                continue
            if exclude_prefixes and any(file_name.lstrip('/').startswith(p) for p in exclude_prefixes):
                logger.info(f"Skipping {file_name} (excluded prefix)")
                continue
            suffixes = file_name.split('.')
            downloadable = False
            for e in extensions:
                e_split = e.split('.')
                if suffixes[-len(e_split):] == e_split:
                    downloadable = True
                    break
            if downloadable:
                files_downloaded.append(file_name.lstrip('/'))
                download_path = os.path.join(base_dir, file_name)
                await download_file(file_name, repo, branch, download_path, session)
                logger.info(f"Download {file_name} complete")
    return files_downloaded

PARALLEL_PARTS_DEFAULT = int(os.environ.get("LAKEFS_DOWNLOAD_PARTS", "8"))
PARALLEL_THRESHOLD_BYTES = int(os.environ.get("LAKEFS_PARALLEL_THRESHOLD", str(64 * 1024 * 1024)))  # 64MiB


async def _stat_size(file_name, repo, branch, session) -> Optional[int]:
    stats_url = (f'{config.lakefs_url}/api/v1/repositories/{urllib.parse.quote_plus(repo)}/refs/'
                 f'{urllib.parse.quote_plus(branch)}/objects/stat?path={file_name}')
    async with session.get(stats_url) as resp:
        if resp.status != 200:
            return None
        data = await resp.json()
        return data.get("size_bytes")


async def download_file(file_name, repo, branch, download_path,
                         session: aiohttp.ClientSession,
                         parts: int = PARALLEL_PARTS_DEFAULT):
    """Download an object from lakefs to disk.

    Strategy:
      - For files >= PARALLEL_THRESHOLD_BYTES: split into `parts` byte ranges,
        fetch in parallel, pwrite into a single pre-sized file. Massive
        speedup for large objects (e.g. 250GB wikidata dumps).
      - For small files: single streaming GET.
    """
    download_dir = os.path.dirname(download_path)
    if download_dir and not os.path.exists(download_dir):
        os.makedirs(download_dir, exist_ok=True)
    file_url = (f'{config.lakefs_url}/api/v1/repositories/{urllib.parse.quote_plus(repo)}/refs/'
                f'{urllib.parse.quote_plus(branch)}/objects?path={file_name}')

    size = await _stat_size(file_name, repo, branch, session)
    if size is None:
        logger.warning(f"File {file_name} not found in {repo}@{branch}")
        return None

    if size < PARALLEL_THRESHOLD_BYTES or parts <= 1:
        logger.info(f"Downloading {file_name} ({size} bytes, single stream)")
        async with session.get(file_url, headers={'Accept-Encoding': 'identity'}) as resp:
            if resp.status != 200:
                logger.warning(f"{file_name} status {resp.status}")
                return None
            with open(download_path, 'wb') as stream:
                async for chunk in resp.content.iter_chunked(1024 * 1024):
                    stream.write(chunk)
        logger.info(f"Download {file_name} complete -> {download_path}")
        return download_path

    logger.info(f"Downloading {file_name} ({size} bytes, {parts} parallel parts)")
    fd = os.open(download_path, os.O_WRONLY | os.O_CREAT | os.O_TRUNC, 0o644)
    try:
        os.ftruncate(fd, size)
        part_size = (size + parts - 1) // parts

        max_attempts = 5
        base_delay   = 2

        async def fetch_part(i: int):
            start = i * part_size
            end = min(start + part_size, size) - 1
            if start > end:
                return
            offset = start  # advances across retries — never re-download bytes already pwritten
            for attempt in range(1, max_attempts + 1):
                if offset > end:
                    return
                headers = {'Accept-Encoding': 'identity', 'Range': f'bytes={offset}-{end}'}
                try:
                    async with session.get(file_url, headers=headers) as resp:
                        if resp.status not in (200, 206):
                            raise Exception(f"part {i} {file_name}: HTTP {resp.status}")
                        async for buf in resp.content.iter_chunked(1024 * 1024):
                            await asyncio.to_thread(os.pwrite, fd, buf, offset)
                            offset += len(buf)
                    return
                except (asyncio.TimeoutError, aiohttp.ClientError, OSError) as e:
                    if attempt >= max_attempts:
                        logger.error(f"part {i} {file_name}: giving up after {attempt} attempts at offset {offset}: {e}")
                        raise
                    delay = base_delay * (2 ** (attempt - 1))
                    logger.warning(
                        f"part {i} {file_name}: attempt {attempt} failed ({type(e).__name__}: {e}), "
                        f"retry in {delay}s (resuming from offset {offset})"
                    )
                    await asyncio.sleep(delay)

        await asyncio.gather(*[fetch_part(i) for i in range(parts)])
    finally:
        os.close(fd)
    logger.info(f"Download {file_name} complete -> {download_path}")
    return download_path


async def download_hdt_files(repo: str, branch: str, kg_name: str, hdt_path: str='hdt') -> None:
    base_dir = config.shared_data_dir + '/deploy'
    # @TODO download into a temp name then rename
    cookie = await login_and_get_cookies(config.lakefs_url, config.lakefs_access_key, config.lakefs_secret_key)
    all_files = []
    connector = aiohttp.TCPConnector(limit_per_host=8)
    timeout = aiohttp.ClientTimeout(total=None, sock_read=600, sock_connect=60)
    async with aiohttp.ClientSession(cookies=cookie, connector=connector, timeout=timeout) as session:
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

async def get_latest_commit(repo: str, ref: str = 'main') -> str:
    """Return the current commit ID for `ref` (branch or tag) in `repo`."""
    cookie = await login_and_get_cookies(config.lakefs_url, config.lakefs_access_key, config.lakefs_secret_key)
    async with aiohttp.ClientSession(cookies=cookie) as session:
        url = (f'{config.lakefs_url}/api/v1/repositories/{urllib.parse.quote_plus(repo)}/refs/'
               f'{urllib.parse.quote_plus(ref)}/commits?amount=1')
        response = await session.get(url)
        if response.status != 200:
            raise Exception(f"Failed to get commit for {repo}@{ref}: HTTP {response.status}")
        data = await response.json()
        results = data.get('results') or []
        if not results:
            raise Exception(f"No commits found for {repo}@{ref}")
        return results[0]['id']


async def download_hdt_files_to_dir(repo: str, ref: str, dest_dir: str, hdt_path: str = 'hdt') -> List[str]:
    """
    Download all .hdt and .hdt.index.v1-1 files from `repo`@`ref` under `hdt_path`
    into `dest_dir`. Files are renamed to `graph.hdt` / `graph.hdt.index.v1-1`
    (uses temp prefix during download then atomic rename).
    Returns list of final paths written.
    """
    os.makedirs(dest_dir, exist_ok=True)
    cookie = await login_and_get_cookies(config.lakefs_url, config.lakefs_access_key, config.lakefs_secret_key)
    all_files = []
    connector = aiohttp.TCPConnector(limit_per_host=8)
    timeout = aiohttp.ClientTimeout(total=None, sock_read=600)
    async with aiohttp.ClientSession(cookies=cookie, connector=connector, timeout=timeout) as session:
        has_more = True
        offset = ""
        while has_more:
            url = (f'{config.lakefs_url}/api/v1/repositories/{urllib.parse.quote_plus(repo)}/refs/'
                   f'{urllib.parse.quote_plus(ref)}/objects/ls?amount=1000&prefix={hdt_path}')
            if offset:
                url += f'&after={offset}'
            response = await session.get(url)
            if response.status != 200:
                raise Exception(f"Error listing {repo}@{ref}/{hdt_path}: HTTP {response.status}")
            results = await response.json()
            has_more = results["pagination"]["has_more"]
            offset = results["pagination"]["next_offset"]
            all_files += [x['path'] for x in results["results"]]

        targets = []
        for file_name in all_files:
            base = file_name.split('/')[-1]
            if base.endswith('.hdt'):
                final_name = 'graph.hdt'
            elif base.endswith('.hdt.index.v1-1'):
                final_name = 'graph.hdt.index.v1-1'
            else:
                continue
            tmp_path = os.path.join(dest_dir, '.tmp.' + final_name)
            final_path = os.path.join(dest_dir, final_name)
            targets.append((file_name, tmp_path, final_path))

        sem = asyncio.Semaphore(4)

        async def _download(file_name, tmp_path):
            async with sem:
                await download_file(file_name, repo, ref, tmp_path, session)

        await asyncio.gather(*[_download(fn, tp) for fn, tp, _ in targets])

        for _, tmp_path, final_path in targets:
            os.replace(tmp_path, final_path)
            logger.info(f"Synced {final_path}")
        return [final_path for _, _, final_path in targets]


async def get_lakefs_prefix_size(repo: str, branch: str, prefix: str) -> int:
    """
    Returns the total size in bytes of all objects under a specific prefix in LakeFS.
    """
    cookie = await login_and_get_cookies(config.lakefs_url, config.lakefs_access_key, config.lakefs_secret_key)
    total_size_bytes = 0
    async with aiohttp.ClientSession(cookies=cookie) as session:
        has_more = True
        offset = ""
        while has_more:
            # We must use quote_plus for repo/branch but prefix must typically be encoded too if it has special chars.
            # Usually prefix="qlever/" is fine with just quote_plus or directly in the URL if simple.
            # Taking inspiration from download_hdt_files
            url = f'{config.lakefs_url}/api/v1/repositories/{urllib.parse.quote_plus(repo)}/refs/{urllib.parse.quote_plus(branch)}/objects/ls?amount=1000&prefix={urllib.parse.quote_plus(prefix)}'
            if offset:
                url += f"&after={offset}"
                
            response = await session.get(url)
            if response.status != 200:
                logger.error(f"Error getting file list for size calculation: {await response.text()}")
                return 0

            results = await response.json()
            has_more = results.get("pagination", {}).get("has_more", False)
            offset = results.get("pagination", {}).get("next_offset", "")
            
            for item in results.get("results", []):
                # Only sum files, not directories (which have size_bytes = 0 usually but good to be safe)
                if item.get("path_type") == "object":
                    total_size_bytes += item.get("size_bytes", 0)

    return total_size_bytes

async def get_latest_tag(repo: str) -> Optional[str]:
    """Highest existing semver tag for a repo (e.g. 'v1.2.3'). Returns None if no tags."""
    client_ = lakefs.client.LakeFSClient(
        configuration=lakefs_sdk.configuration.Configuration(
            config.lakefs_url,
            username=config.lakefs_access_key,
            password=config.lakefs_secret_key
        )
    )
    results = client_.tags_api.list_tags(repo)
    pagination = results.pagination
    tags = list(results.results)
    while pagination.has_more:
        results = client_.tags_api.list_tags(repo, after=pagination.next_offset)
        pagination = results.pagination
        tags += list(results.results)
    if not tags:
        return None
    versions = [t.id.lstrip('v') for t in tags]
    latest = get_latest_version(versions)
    return f"v{latest}" if latest else None


async def resolve_future_tag(repo: str) -> str:
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
    return latest_tag

async def upload_files(repo: str, root_branch: str = "main", local_files: list[tuple[str, str]] = None):
    """Upload the result and clear dir"""
    latest_tag = await resolve_future_tag(repo)
    stable_branch_name = f"stable_{latest_tag.replace('.', '_')}"
    # create client
    client = lakefs.client.LakeFSClient(
        configuration=lakefs_sdk.configuration.Configuration(
            config.lakefs_url,
            username=config.lakefs_access_key,
            password=config.lakefs_secret_key
        )
    )
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
            stream = await open_file_with_retry(file, mode='rb')
            with stream:
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
    if len(local_files):
        # Commit
        try:
            client.commits_api.commit(repository=repo, branch=stable_branch_name, commit_creation={
                "message": f"Uploads for version {latest_tag}",
                "metadata": {
                    "key": "value"
                }
            })
        except BadRequestException as e:
            b = json.loads(e.body)
            if b['message'] != "commit: no changes":
                raise e


    return {
        "stable_branch_name": stable_branch_name,
        "future_tag": latest_tag,
    }


def clean_up_files(repo: str):
    base_dir = config.local_data_dir + '/' + repo
    if not os.path.exists(base_dir):
        return
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


async def open_file_with_retry(filepath: str, mode: str = "rb", retries: int = 10, initial_delay: float = 1):
    delay = initial_delay
    for attempt in range(1, retries + 1):
        try:
            stream = open(filepath, mode)
            logger.info(f"Opened file {filepath} after {attempt} attempts")
            return stream
        except Exception as e:
            logger.error(f"Error opening file {filepath} ... retrying in {delay}")
            if attempt == retries:
                raise e
            await asyncio.sleep(delay)
            delay *= 2


async def object_exists(repo: str, ref: str, remote_file_path: str) -> bool:
    """Lakefs `objects/stat` probe. Returns True iff the object is present
    at `ref` with a non-None size. Used to pre-flight downloads so missing
    sources can be reported up front instead of failing mid-build."""
    cookie = await login_and_get_cookies(config.lakefs_url, config.lakefs_access_key, config.lakefs_secret_key)
    timeout = aiohttp.ClientTimeout(total=30, sock_read=15, sock_connect=10)
    async with aiohttp.ClientSession(cookies=cookie, timeout=timeout) as session:
        size = await _stat_size(remote_file_path, repo, ref, session)
        return size is not None


async def download_file_at_ref(repo: str, ref: str, remote_file_path: str, local_download_path: str):
    """Download a single object from `repo` at an explicit ref (branch/tag/commit).

    Uses aiohttp ClientTimeout with no overall total cap and generous per-socket
    read/connect windows; default aiohttp total=5min times out mid-stream for
    multi-GB objects (e.g. wikidata graph.nt.gz).
    """
    cookie = await login_and_get_cookies(config.lakefs_url, config.lakefs_access_key, config.lakefs_secret_key)
    connector = aiohttp.TCPConnector(limit_per_host=8)
    timeout = aiohttp.ClientTimeout(total=None, sock_read=600, sock_connect=60)
    async with aiohttp.ClientSession(cookies=cookie, connector=connector, timeout=timeout) as session:
        response = await download_file(remote_file_path, repo, ref, local_download_path, session)
        if not response:
            return f"{repo}"


async def download_file_from_latest_tag(repo: str, remote_file_path: str, local_download_path: str):
    """
    Downloads a specific file from the latest tag of the specified repository.

    :param repo: The name of the repository.
    :param remote_file_path: The path of the file in the repository.
    :param local_download_path: The local path where the file should be saved.
    """
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
    tags_list = results.results
    while pagination.has_more:
        results = client.tags_api.list_tags(repo, after=pagination.next_offset)
        pagination = results.pagination
        tags_list.extend(results.results)

    if not tags_list:
        raise Exception(f"No tags found for repository {repo}")

    versions = [tag.id.lstrip('v') for tag in tags_list]
    latest_version = get_latest_version(versions)
    latest_tag = f"v{latest_version}"

    logger.info(f"Latest tag for {repo} is {latest_tag}. Downloading {remote_file_path}...")

    cookie = await login_and_get_cookies(config.lakefs_url, config.lakefs_access_key, config.lakefs_secret_key)
    connector = aiohttp.TCPConnector(limit_per_host=8)
    timeout = aiohttp.ClientTimeout(total=None, sock_read=600, sock_connect=60)
    async with aiohttp.ClientSession(cookies=cookie, connector=connector, timeout=timeout) as session:
        response = await download_file(remote_file_path, repo, latest_tag, local_download_path, session)
        if not response:
            return f"{repo}"


if __name__ == '__main__':

    from models.kg_metadata import KGConfig
    kgs = KGConfig.from_git_sync().kgs
    import asyncio
    errors = []
    skip_list = [] #['ubergraph', 'wikidata', 'sem-open-alex-kg']
    # for kg in kgs:
    #     if kg.frink_options and kg.frink_options.lakefs_repo not in skip_list:
    #         print(f'***** {kg.frink_options.lakefs_repo} ****')
    #         err = asyncio.run(download_file_from_latest_tag(kg.frink_options.lakefs_repo, "nt/graph.nt.gz", "local"))
    #         errors.append(err)
    #         print("*8888***"*10)
    #      # upload_files("test-hook-repo", root_branch= "main", local_files=[('/home/kebedey/projects/frink/kace/README.MD', 'test_file')]))
    # print([x for x in errors if x])
    asyncio.run(
        download_files("biobricks-mesh-kg", "v0.0.1")
    )