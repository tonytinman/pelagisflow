"""
Nova Framework Contract Module

Provides data contract loading, management, and generation.
"""

from nova_framework.contract.contract import DataContract
from nova_framework.contract.contract_generator import (
    ContractGenerator,
    get_enabled_classifications,
    get_disabled_classifications,
    CLASSIFICATION_TOGGLES,
    # PII classification toggle constants
    INCLUDE_PII,
    INCLUDE_QUASI_PII,
    INCLUDE_SPECIAL,
    INCLUDE_CRIMINAL,
    INCLUDE_CHILD,
    INCLUDE_FINANCIAL_PII,
    INCLUDE_PCI,
    INCLUDE_AUTH,
    INCLUDE_LOCATION,
    INCLUDE_TRACKING,
    INCLUDE_HR,
    INCLUDE_COMMERCIAL,
    INCLUDE_IP,
)

__all__ = [
    "DataContract",
    "ContractGenerator",
    "get_enabled_classifications",
    "get_disabled_classifications",
    "CLASSIFICATION_TOGGLES",
    "INCLUDE_PII",
    "INCLUDE_QUASI_PII",
    "INCLUDE_SPECIAL",
    "INCLUDE_CRIMINAL",
    "INCLUDE_CHILD",
    "INCLUDE_FINANCIAL_PII",
    "INCLUDE_PCI",
    "INCLUDE_AUTH",
    "INCLUDE_LOCATION",
    "INCLUDE_TRACKING",
    "INCLUDE_HR",
    "INCLUDE_COMMERCIAL",
    "INCLUDE_IP",
]