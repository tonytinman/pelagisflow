"""
NovaFlow Data Access Control and Privacy Module.

This module provides:
1. Table-level differential access control enforcement (GRANT/REVOKE)
2. Column-level masking with role-based access control

Both are driven by declarative metadata from data contracts and role mappings.

Key Components:

Access Control (Table-level):
- Data models for privileges and deltas
- Metadata loading from contract registry
- UC privilege inspection
- Differential analysis (intent vs actual)
- GRANT/REVOKE execution

Privacy/Masking (Column-level):
- Privacy classification models (PII, quasi, vulnerable)
- Role-based masking with AD group exemptions
- Masking functions (hash, redact, partial, etc.)
- UC masking policy inspection
- Differential masking enforcement

Usage:
    # In pipeline (automatic)
    from nova_framework.pipeline.stages import AccessControlStage, PrivacyStage

    # Standalone (on-demand)
    from nova_framework.access import StandaloneAccessControlTool

    tool = StandaloneAccessControlTool(spark, environment="dev")

    # Access control only
    tool.apply_to_table(catalog, schema, table)

    # Privacy/masking only
    tool.apply_privacy_to_table(contract, catalog)

    # Full security (access + privacy)
    access_result, privacy_result = tool.apply_full_security(contract, catalog)
"""

# Access Control (Table-level)
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
from .grant_revoker import GrantRevoker

# Privacy/Masking (Column-level)
from .privacy_models import (
    PrivacyClassification,
    MaskingStrategy,
    ColumnPrivacyMetadata,
    UCMaskingPolicy,
    MaskingIntent,
    MaskingDelta,
    PrivacyEnforcementResult,
    DEFAULT_MASKING_STRATEGIES
)

from .masking_functions import MaskingFunctions
from .uc_masking_inspector import UCMaskingInspector
from .privacy_metadata_loader import PrivacyMetadataLoader
from .privacy_engine import PrivacyEngine

# Standalone Tool (Access + Privacy)
from .standalone import StandaloneAccessControlTool

__version__ = "1.1.0"

__all__ = [
    # ============================================================
    # ACCESS CONTROL (Table-level)
    # ============================================================

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
    'GrantRevoker',

    # ============================================================
    # PRIVACY/MASKING (Column-level)
    # ============================================================

    # Enums
    'PrivacyClassification',
    'MaskingStrategy',

    # Data models
    'ColumnPrivacyMetadata',
    'UCMaskingPolicy',
    'MaskingIntent',
    'MaskingDelta',
    'PrivacyEnforcementResult',

    # Constants
    'DEFAULT_MASKING_STRATEGIES',

    # Core components
    'MaskingFunctions',
    'UCMaskingInspector',
    'PrivacyMetadataLoader',
    'PrivacyEngine',

    # ============================================================
    # TOOLS
    # ============================================================

    'StandaloneAccessControlTool',  # Access + Privacy
]