"""
NovaFlow Data Access Control Module.

This module provides table-level differential access control enforcement
for Unity Catalog, driven by declarative metadata.

Key Components:
- Data models for privileges and deltas
- Metadata loading from contract registry
- UC privilege inspection
- Differential analysis (intent vs actual)
- GRANT/REVOKE execution
- Standalone enforcement tool

Usage:
    # In pipeline (automatic)
    from nova_framework.access.stage import AccessControlStage
    
    # Standalone (on-demand)
    from nova_framework.access import StandaloneAccessControlTool
    
    tool = StandaloneAccessControlTool(spark, environment="dev")
    tool.apply_to_table(catalog, schema, table)
"""

from .models import (
    UCPrivilege,
    PrivilegeIntent,
    ActualPrivilege,
    PrivilegeDelta,
    AccessControlResult
)

from .metadata_loader import AccessMetadataLoader
from .uc_inspector import UCPrivilegeInspector
from .delta_generator import PrivilegeDeltaGenerator

# Note: GrantRevoker and StandaloneAccessControlTool imports temporarily disabled
# grant_revoker.py currently has wrong content (duplicate of delta_generator)
# Uncomment when GrantRevoker is properly implemented:
# from .grant_revoker import GrantRevoker
# from .standalone import StandaloneAccessControlTool

__version__ = "1.0.0"

__all__ = [
    # Enums
    'UCPrivilege',

    # Data models
    'PrivilegeIntent',
    'ActualPrivilege',
    'PrivilegeDelta',
    'AccessControlResult',

    # Core components
    'AccessMetadataLoader',
    'UCPrivilegeInspector',
    'PrivilegeDeltaGenerator',

    # Temporarily disabled until GrantRevoker is properly implemented:
    # 'GrantRevoker',
    # 'StandaloneAccessControlTool',
]