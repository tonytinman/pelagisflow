"""
Execution context for Nova Framework pipelines.

Provides runtime context including configuration, contract, and
execution metadata.
"""

from dataclasses import dataclass, field
from datetime import datetime
from typing import Optional, Dict, Any
from nova_framework.contract.contract import DataContract
from nova_framework.core.config import FrameworkConfig, get_config


@dataclass
class ExecutionContext:
    """
    Runtime context for pipeline execution.
    
    Provides access to:
    - Configuration
    - Data contract
    - Execution metadata
    - Derived properties (paths, catalog names, etc.)
    """
    
    # Execution parameters
    process_queue_id: int
    data_contract_name: str
    source_ref: str
    env: str
    
    # Configuration
    config: FrameworkConfig = field(default_factory=get_config)
    
    # Runtime state
    contract: Optional[DataContract] = None
    created_at: datetime = field(default_factory=datetime.now)
    
    # Additional context (for stages to store data)
    state: Dict[str, Any] = field(default_factory=dict)
    
    def __post_init__(self):
        """Initialize runtime dependencies."""
        # Load contract
        if self.data_contract_name:
            self.contract = DataContract(self.data_contract_name, self.env)
    
    @property
    def catalog(self) -> str:
        """Get catalog name for current environment."""
        return self.config.catalog.get_catalog_name(self.env)
    
    @property
    def data_file_path(self) -> str:
        """Get data file path from contract and config."""
        if not self.contract:
            raise ValueError("Contract not loaded")
        
        return self.config.storage.get_data_path(
            catalog=self.catalog,
            schema=self.contract.schema_name,
            table=self.contract.table_name,
            source_ref=self.source_ref
        )
    
    @property
    def target_table(self) -> str:
        """Get fully qualified target table name."""
        if not self.contract:
            raise ValueError("Contract not loaded")
        
        return self.config.catalog.get_table_name(
            catalog=self.catalog,
            schema=self.contract.schema_name,
            table=self.contract.table_name
        )
    
    def set_state(self, key: str, value: Any):
        """Store state for use across stages."""
        self.state[key] = value
    
    def get_state(self, key: str, default: Any = None) -> Any:
        """Retrieve state stored by stages."""
        return self.state.get(key, default)


# Backward compatibility alias
PipelineContext = ExecutionContext