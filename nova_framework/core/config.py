"""
Configuration management for Nova Framework.

Centralizes all configuration including catalog names, table paths,
environment settings, and feature flags.
"""

from dataclasses import dataclass, field
from typing import Dict, Optional
import os


@dataclass
class CatalogConfig:
    """Configuration for Unity Catalog."""
    
    organization: str = "cluk"  # Organization prefix
    framework_name: str = "nova"
    
    def get_catalog_name(self, env: str) -> str:
        """Get catalog name for environment."""
        return f"{self.organization}_{env}_{self.framework_name}"
    
    def get_schema_name(self, catalog: str, schema: str) -> str:
        """Get fully qualified schema name."""
        return f"{catalog}.{schema}"
    
    def get_table_name(self, catalog: str, schema: str, table: str) -> str:
        """Get fully qualified table name."""
        return f"{catalog}.{schema}.{table}"


@dataclass
class StorageConfig:
    """Configuration for storage locations."""
    
    base_volume_template: str = "/Volumes/{catalog}/{schema}/raw/{source_ref}/{table}/"
    quarantine_path_template: str = "/Volumes/{catalog}/nova_framework/quarantine/{date}/"
    
    def get_data_path(
        self,
        catalog: str,
        schema: str,
        table: str,
        source_ref: str
    ) -> str:
        """Get data file path."""
        return self.base_volume_template.format(
            catalog=catalog,
            schema=schema,
            source_ref=source_ref,
            table=table
        )


@dataclass
class ObservabilityConfig:
    """Configuration for observability features."""
    
    # Logging
    log_level: str = "INFO"
    log_to_delta: bool = True
    log_table: str = "nova_framework.logs"
    
    # Metrics
    metrics_enabled: bool = True
    metrics_table: str = "nova_framework.metrics"
    
    # Telemetry
    telemetry_enabled: bool = True
    telemetry_table: str = "nova_framework.telemetry"
    
    # Statistics
    stats_table: str = "nova_framework.pipeline_stats"

    # Data Quality
    dq_errors_table: str = "nova_framework.dq_errors"


@dataclass
class FrameworkConfig:
    """
    Main framework configuration.
    
    This is the single source of truth for all configuration.
    """
    
    # Environment
    env: str = field(default_factory=lambda: os.getenv("ENV", "dev"))
    
    # Sub-configurations
    catalog: CatalogConfig = field(default_factory=CatalogConfig)
    storage: StorageConfig = field(default_factory=StorageConfig)
    observability: ObservabilityConfig = field(default_factory=ObservabilityConfig)
    
    # Feature flags
    enable_soft_delete: bool = True
    enable_schema_evolution: bool = True
    enable_data_quality: bool = True
    
    def get_catalog_name(self) -> str:
        """Get catalog name for current environment."""
        return self.catalog.get_catalog_name(self.env)
    
    @classmethod
    def from_env(cls, env: str) -> "FrameworkConfig":
        """Create configuration for specific environment."""
        return cls(env=env)
    
    @classmethod
    def from_dict(cls, config_dict: Dict) -> "FrameworkConfig":
        """Create configuration from dictionary."""
        return cls(**config_dict)


# Global configuration instance
_config: Optional[FrameworkConfig] = None


def get_config() -> FrameworkConfig:
    """Get global configuration instance."""
    global _config
    if _config is None:
        _config = FrameworkConfig()
    return _config


def set_config(config: FrameworkConfig):
    """Set global configuration instance."""
    global _config
    _config = config


def reset_config():
    """Reset global configuration to None."""
    global _config
    _config = None