from pydantic import BaseModel


class LakefsMergeActionModel(BaseModel):
    event_type: str
    event_time: str
    action_name: str
    hook_id: str
    repository_id: str
    branch_id: str
    source_ref: str
    commit_message: str
    commit_id: str
    committer: str
    commit_metadata: dict

class LakefTagCreationModel(BaseModel):
    event_type: str
    event_time: str
    action_name: str
    hook_id: str
    repository_id: str
    source_ref: str
    tag_id: str
    commit_id: str

