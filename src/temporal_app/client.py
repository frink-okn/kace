from temporalio.client import Client
from config import config

async def get_client() -> Client:
    """
    Connects to the Temporal server using configuration from config.py.
    """
    return await Client.connect(
        config.temporal_host,
        namespace=config.temporal_namespace,
    )
