from typing import List, Dict, Optional, Union
from pydantic import BaseModel, Field, PrivateAttr,field_validator
import yaml
import aiohttp
from config import config

# Define a model for the nested "contact" information
class Contact(BaseModel):
    email: Optional[List[str]] = []
    github: Optional[List[str]] = []
    label: Optional[str] = ""

    @field_validator("email", "github", mode="before")
    @classmethod
    def parse_comma_or_list(cls, value):
        if value is None:
            return []
        if isinstance(value, str):
            # Split on commas and strip spaces
            return [v.strip() for v in value.split(",") if v.strip()]
        elif isinstance(value, list):
            # Ensure all elements are strings and stripped
            return [str(v).strip() for v in value if str(v).strip()]
        return value
# Define a model for the nested "frink-options"
class FrinkOptions(BaseModel):
    documentation_path: Optional[str] = Field(alias="documentation-path")
    lakefs_repo: Optional[str] = Field(alias="lakefs-repo")
    neo4j_conversion_config_path: Optional[str] = Field(alias="neo4j-conversion-config-path", default="")

# Define a model for each KG item
class KG(BaseModel):
    contact: Contact
    description: str
    frink_options: Optional[FrinkOptions] = Field(None, alias="frink-options")
    funding: Optional[str] = None
    homepage: Optional[str] = None
    shortname: Optional[str] = None
    sparql: Optional[str] = None
    template: Optional[str] = None
    title: Optional[str] = None
    tpf: Optional[str] = None
    stats: Optional[str] = None

# Define a container model for the entire YAML structure
class KGConfig(BaseModel):
    kgs: List[KG]
    _by_key: Dict[str, KG] = PrivateAttr()

    @staticmethod
    async def from_git():
        async with aiohttp.ClientSession() as session:
            response = await session.get(config.kg_config_url)
            kgs = yaml.safe_load(await response.text())
            return KGConfig(**kgs)



    def __init__(self, **data):
        super().__init__(**data)
        # Build the lookup dictionary only for items with a lakefs_repo
        self._by_key = {
            kg.frink_options.lakefs_repo: kg
            for kg in self.kgs
            if kg.frink_options and kg.frink_options.lakefs_repo
        }

    def get_by_repo(self, repo_id: str) -> Optional[KG]:
        return self._by_key.get(repo_id)

# Example usage:
if __name__ == "__main__":
    # Your YAML content as a string
    import asyncio

    # Create the KGConfig instance from the dict
    config = asyncio.run(KGConfig.from_git())

    # Now you can lookup KG items by their lakefs_repo value
    print(config.get_by_repo("urban-flooding-open-knowledge-network"))
    bioheath = config.get_by_repo("biohealth")
    print(config.get_by_repo("biohealth").frink_options)
    # print(config.get_by_repo("dream-kg").contact.emil)

