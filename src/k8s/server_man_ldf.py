from log_util import LoggingUtil
from k8s.server_man import ServerDeploymentManager
import yaml
from typing import Dict, Any
from models.kg_metadata import KGConfig
import os
from config import config as app_config
import json
import asyncio

logger = LoggingUtil.init_logging("ldf-k8s-man")

class LDFServerDeploymentMananger(ServerDeploymentManager):
    def __init__(self, templates_dir, namespace):
        super().__init__(templates_dir, namespace)


    def get_config_map(self, parameters: Dict[str, Any]) -> Dict:
        config_map_template = self.templates.get_template("config-map.j2")
        kg_details = [
            {"title": "Wikidata", "data_source_path": "wikidata", "hdt_file_name": "wikidata-all.hdt"},
            {"title": "Ubergraph", "data_source_path": "ubergraph", "hdt_file_name": "ubergraph.hdt"},
        ]
        kgConfig: KGConfig = KGConfig.from_git_sync()
        for kg in kgConfig.kgs:
            if kg.frink_options:
                is_added = kg.shortname in [x['data_source_path'] for x in kg_details]
                if not is_added:
                    kg_details.append(
                        {
                            "title": kg.title,
                            "data_source_path": kg.shortname,
                            "hdt_file_name": kg.shortname + '.hdt'
                        }
                    )
            else:
                print(kg)
        data_source_config = [LDFServerDeploymentMananger._make_data_source_entry(**kg_detail)
                              for kg_detail in kg_details]
        parameters.update({
            "datasources": data_source_config
        })
        return yaml.safe_load(config_map_template.render(parameters))

    @staticmethod
    def _make_data_source_entry(title, data_source_path, hdt_file_name, hdt_root_path="/data/deploy"):
        hdt_root_path = hdt_root_path.rstrip('/')
        return {
              "@id": f"urn:ldf-server:{data_source_path}Datasource",
              "@type": "HdtDatasource",
              "datasourceTitle": f"{title}",
              "description": f"{title} Triple Pattern Fragments endpoint",
              "datasourcePath": f"{data_source_path}",
              "hdtFile": f"{hdt_root_path}/{hdt_file_name}"
        }


if __name__ == "__main__":
    LDF_TEMPLATE_DIR = (
            os.path.dirname(os.path.realpath(__file__)) +
            os.path.join(os.path.sep + "templates", "ldf")
    )
    man = LDFServerDeploymentMananger(
        LDF_TEMPLATE_DIR, app_config.k8s_namespace
    )
    config_map = man.get_config_map(
        {
            "kg_name": "all",
            "host_name": "frink.apps.renci.org"
        }
    )

    print(config_map['data']['config.json'])