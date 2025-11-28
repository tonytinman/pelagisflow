from dataclasses import dataclass, field
from datetime import datetime
from nova_framework.contract.contract import DataContract
from nova_framework.observability.pipeline_stats import PipelineStats

@dataclass
class PipelineContext:
    """
    Runtime context for a Nova Framework pipeline execution.

    Structurally immutable: once built, core references (Spark, tracker, telemetry)
    should not be reassigned, but can be mutated internally as they operate.

    Supports post-init auto-creation of runtime dependencies (Spark, UC, etc.).
    """

    process_queue_id: int
    data_contract_name: str
    source_ref: str
    env: str

    stats: PipelineStats = field(default_factory=PipelineStats)
    instance_created: datetime = field(default_factory=datetime.now)
   
    def __post_init__(self):
        """Build runtime dependencies if not injected."""
        self.catalog = f"cluk_{self.env}_nova"

        # Load data contract
        if self.data_contract_name is not None:
            # contract loads itself in constructor
            self.contract = DataContract(self.data_contract_name,self.env)

    @property
    def data_file_path(self) -> str:
        """Construct data file path from contract"""
        data_file_path = f"/Volumes/cluk_{self.env}_nova/{self.contract.schema_name}/raw/{self.source_ref}/{self.contract.table_name}/"
        return data_file_path
   
    @property
    def violations_schema(self) -> str:
        """Get schema for violations DataFrame"""
        return f"{self.contract.schema.lower()}_violations"
