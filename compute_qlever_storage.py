import asyncio
import lakefs
import lakefs_sdk
from models import kg_metadata
from config import config
from lakefs_util.io_util import get_lakefs_prefix_size
from lakefs_util.semver_util import get_latest_version

def get_latest_tag_or_main(repo: str) -> str:
    # create client
    client = lakefs.client.LakeFSClient(
        configuration=lakefs_sdk.configuration.Configuration(
            config.lakefs_url,
            username=config.lakefs_access_key,
            password=config.lakefs_secret_key
        )
    )
    # get all tags
    try:
        results = client.tags_api.list_tags(repo)
        pagination = results.pagination
        tags = results.results
        while pagination.has_more:
            results = client.tags_api.list_tags(repo, after=pagination.next_offset)
            pagination = results.pagination
            tags.extend(results.results)
        
        versions = [tag.id.lstrip('v') for tag in tags if tag.id.startswith('v')]
        if not versions:
            return "main"
        
        latest_version = get_latest_version(versions)
        return "v" + latest_version
    except Exception as e:
        print(f"Could not fetch tags for {repo}, defaulting to main. Error: {e}")
        return "main"

async def compute_qlever_storage():
    kg_config = kg_metadata.KGConfig.from_git_sync()
    total_size_bytes = 0
    
    print(f"{'Repository':<30} | {'Tag / Branch':<15} | {'Size (GB)':<12}")
    print("-" * 65)
    
    for kg in kg_config.kgs:
        if not kg.frink_options or not kg.frink_options.lakefs_repo:
            continue
            
        lakefs_repo = kg.frink_options.lakefs_repo
        latest_tag = get_latest_tag_or_main(lakefs_repo)
        
        # calculate size for qlever prefix
        # By passing 'qlever/' we get all index files within the directory
        size_bytes = await get_lakefs_prefix_size(lakefs_repo, latest_tag, "qlever/")
        size_gb = size_bytes / (1024**3)
        total_size_bytes += size_bytes
        
        print(f"{lakefs_repo:<30} | {latest_tag:<15} | {size_gb:10.4f}")
        
    total_size_gb = total_size_bytes / (1024**3)
    print("-" * 65)
    print(f"{'Total':<48} {total_size_gb:10.4f} GB")

if __name__ == '__main__':
    asyncio.run(compute_qlever_storage())
