import hashlib
import json
import os
from typing import Dict, Any, List, Optional

import yaml
from kubernetes import client
from kubernetes.client.rest import ApiException

from log_util import LoggingUtil
from k8s.server_man import ServerDeploymentManager
from models.kg_metadata import KGConfig
from config import config as app_config

logger = LoggingUtil.init_logging("ldf-k8s-man")


HDT_ROOT_PATH = "/data/deploy"


class LDFServerDeploymentMananger(ServerDeploymentManager):
    def __init__(self, templates_dir, namespace):
        super().__init__(templates_dir, namespace)

    @staticmethod
    def _ds(title: str, slug: str, hdt_file_path: str) -> Dict[str, Any]:
        return {
            "@id": f"urn:ldf-server:{slug}Datasource",
            "@type": "HdtDatasource",
            "datasourceTitle": title,
            "description": f"{title} Triple Pattern Fragments endpoint",
            "datasourcePath": slug,
            "hdtFile": hdt_file_path,
        }

    def build_datasources(self, kg_config: Optional[KGConfig] = None) -> List[Dict[str, Any]]:
        """Build LDF datasource list: extras first (wikidata, etc.), then registry KGs."""
        datasources: List[Dict[str, Any]] = []
        seen_slugs = set()
        for extra in app_config.ldf_extra_datasources or []:
            slug = extra["data_source_path"]
            hdt_file = extra["hdt_file_name"]
            hdt_path = f"{HDT_ROOT_PATH}/{hdt_file}" if not hdt_file.startswith("/") else hdt_file
            datasources.append(self._ds(extra["title"], slug, hdt_path))
            seen_slugs.add(slug)

        if kg_config is None:
            kg_config = KGConfig.from_git_sync()
        for kg in kg_config.kgs:
            if not kg.frink_options or not kg.shortname:
                continue
            slug = kg.shortname
            if slug in seen_slugs:
                continue
            datasources.append(
                self._ds(kg.title or slug, slug, f"{HDT_ROOT_PATH}/{slug}/graph.hdt")
            )
            seen_slugs.add(slug)
        return datasources

    def get_config_map(self, parameters: Dict[str, Any]) -> Dict:
        config_map_template = self.templates.get_template("config-map.j2")
        kg_config: Optional[KGConfig] = parameters.pop("kg_config", None)
        datasources = self.build_datasources(kg_config)
        parameters.update({
            "datasources": datasources,
            "host_name": parameters.get("host_name") or app_config.ldf_host_name,
        })
        return yaml.safe_load(config_map_template.render(parameters))

    def get_pvc(self) -> Dict:
        tmpl = self.templates.get_template("pvc.j2")
        return yaml.safe_load(tmpl.render({
            "pvc_name": app_config.ldf_pvc_name,
            "pvc_size": app_config.ldf_pvc_size,
            "storage_class": app_config.ldf_pvc_storage_class,
        }))

    def get_state_configmap(self, commits: Dict[str, str]) -> Dict:
        tmpl = self.templates.get_template("state-configmap.j2")
        # Use json.dumps + indent so the |- block stays valid YAML.
        commits_json = json.dumps(commits, indent=2).replace("\n", "\n    ")
        return yaml.safe_load(tmpl.render({"commits_json": commits_json}))

    def get_sync_job(self, repo: str, ref: str, shortname: str, image: str,
                     env_pairs: List[Dict[str, str]], hdt_path: str = "hdt",
                     job_name: Optional[str] = None) -> Dict:
        tmpl = self.templates.get_template("sync-job.j2")
        if not job_name:
            job_name = f"ldf-sync-{shortname}-{ref[:8]}"
        return yaml.safe_load(tmpl.render({
            "job_name": job_name,
            "repo": repo,
            "ref": ref,
            "shortname": shortname,
            "image": image,
            "hdt_path": hdt_path,
            "pvc_name": app_config.ldf_pvc_name,
            "env": env_pairs,
        }))

    def get_deployment(self, parameters: Dict[str, Any]) -> Dict:
        deployment_template = self.templates.get_template("server-deployment.j2")
        parameters.setdefault("pvc_name", app_config.ldf_pvc_name)
        parameters.setdefault("replicas", app_config.ldf_replicas)
        parameters.setdefault("image", app_config.ldf_image)
        return yaml.safe_load(deployment_template.render(parameters))

    @staticmethod
    def compute_config_hash(datasources: List[Dict[str, Any]]) -> str:
        h = hashlib.sha256(json.dumps(datasources, sort_keys=True).encode()).hexdigest()
        return h[:16]

    def apply_pvc(self) -> None:
        body = self.get_pvc()
        name = body["metadata"]["name"]
        api = client.CoreV1Api()
        try:
            api.read_namespaced_persistent_volume_claim(name=name, namespace=self.namespace)
        except ApiException as e:
            if e.status == 404:
                api.create_namespaced_persistent_volume_claim(namespace=self.namespace, body=body)
            else:
                raise

    def apply_state_configmap(self, commits: Dict[str, str]) -> None:
        body = self.get_state_configmap(commits)
        name = body["metadata"]["name"]
        api = client.CoreV1Api()
        try:
            api.read_namespaced_config_map(name=name, namespace=self.namespace)
            api.patch_namespaced_config_map(name=name, namespace=self.namespace, body=body)
        except ApiException as e:
            if e.status == 404:
                api.create_namespaced_config_map(namespace=self.namespace, body=body)
            else:
                raise

    def read_state_configmap(self) -> Dict[str, str]:
        api = client.CoreV1Api()
        try:
            cm = api.read_namespaced_config_map(name="frink-ldf-state", namespace=self.namespace)
            data = cm.data or {}
            raw = data.get("commits.json", "{}")
            return json.loads(raw)
        except ApiException as e:
            if e.status == 404:
                return {}
            raise

    def submit_sync_job(self, repo: str, ref: str, shortname: str, image: str,
                        env_pairs: List[Dict[str, str]], hdt_path: str = "hdt") -> str:
        body = self.get_sync_job(repo, ref, shortname, image, env_pairs, hdt_path)
        api = client.BatchV1Api()
        name = body["metadata"]["name"]
        # Delete any prior Job with same name so we can resubmit
        try:
            api.delete_namespaced_job(name=name, namespace=self.namespace,
                                      propagation_policy="Background")
        except ApiException as e:
            if e.status != 404:
                raise
        api.create_namespaced_job(namespace=self.namespace, body=body)
        return name

    def wait_for_job(self, job_name: str, timeout_seconds: int = 3600) -> bool:
        import time
        api = client.BatchV1Api()
        deadline = time.time() + timeout_seconds
        while time.time() < deadline:
            job = api.read_namespaced_job(name=job_name, namespace=self.namespace)
            status = job.status
            if status.succeeded:
                return True
            if status.failed:
                raise Exception(f"Job {job_name} failed: {status.conditions}")
            time.sleep(5)
        raise TimeoutError(f"Job {job_name} did not finish in {timeout_seconds}s")

    def rolling_restart(self, config_hash: str) -> None:
        """Patch the LDF deployment so that the pod template config-hash annotation
        changes — triggers a controlled rolling update."""
        api = client.AppsV1Api()
        body = {
            "spec": {
                "template": {
                    "metadata": {
                        "annotations": {"kace/config-hash": config_hash}
                    }
                }
            }
        }
        api.patch_namespaced_deployment(name="frink-ldf-server",
                                        namespace=self.namespace, body=body)


if __name__ == "__main__":
    LDF_TEMPLATE_DIR = (
        os.path.dirname(os.path.realpath(__file__))
        + os.path.join(os.path.sep + "templates", "ldf")
    )
    man = LDFServerDeploymentMananger(LDF_TEMPLATE_DIR, app_config.k8s_namespace)
    config_map = man.get_config_map({"kg_name": "all", "host_name": "frink.apps.renci.org"})
    print(config_map["data"]["config.json"])
