from pydantic import BaseModel
from typing import List, Dict, Any
class NodeConfig(BaseModel): id: str; type: str; config: Dict[str, Any]; inputs: List[str] = []
class PipelineConfig(BaseModel): id: str = "default"; nodes: List[NodeConfig]
class RunResponse(BaseModel): status: str; message: str; rows_processed: int; duration_sec: float; output_table: str
class SQLQuery(BaseModel): query: str
