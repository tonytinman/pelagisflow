"""
Data models for NovaFlow Data Privacy and Column Masking.

This module defines the core data structures for privacy classifications,
masking strategies, and role-based masking policies.
"""

from dataclasses import dataclass, field
from typing import List, Optional, Dict, Set
from enum import Enum


class PrivacyClassification(Enum):
    """
    Privacy/sensitivity classification for data columns.

    Maps to data contract 'privacy' field values.
    Aligned with GDPR categories and industry standards.

    GDPR Categories (Art. 4, 9, 10):
    - PII: Basic Personal Data (Direct identifiers) - Art. 4
    - QUASI_PII: Quasi-Personal Data (Indirect identifiers) - Art. 4
    - SPECIAL: Special Category Personal Data - Art. 9
    - CRIMINAL: Criminal Offence Data - Art. 10
    - CHILD: Children's Personal Data - Art. 8

    Industry/Risk-based Categories:
    - FINANCIAL_PII: Financial Personal Data (sector regulations)
    - PCI: Payment Card Data (PCI DSS)
    - AUTH: Authentication & Security Data (Art. 32)
    - LOCATION: Precise Location Data
    - TRACKING: Online Identifiers / Tracking Data
    - HR: Employment / HR Data
    - COMMERCIAL: Commercially Sensitive Financial Data (non-personal)
    - IP: Intellectual Property / Trade Secrets (non-personal)
    """
    NONE = "none"

    # GDPR Personal Data Categories
    PII = "pii"  # Basic Personal Data (Direct identifiers)
    QUASI_PII = "quasi_pii"  # Quasi-Personal Data (Indirect identifiers)
    SPECIAL = "special"  # Special Category Personal Data (Art. 9)
    CRIMINAL = "criminal"  # Criminal Offence Data (Art. 10)
    CHILD = "child"  # Children's Personal Data (Art. 8)

    # Industry/Risk-based Categories
    FINANCIAL_PII = "financial_pii"  # Financial Personal Data
    PCI = "pci"  # Payment Card Data (PCI DSS)
    AUTH = "auth"  # Authentication & Security Data
    LOCATION = "location"  # Precise Location Data
    TRACKING = "tracking"  # Online Identifiers / Tracking Data
    HR = "hr"  # Employment / HR Data

    # Non-personal but sensitive
    COMMERCIAL = "commercial"  # Commercially Sensitive Financial Data
    IP = "ip"  # Intellectual Property / Trade Secrets

    # Legacy aliases (for backward compatibility)
    QUASI = "quasi_pii"  # Deprecated: use QUASI_PII
    VULNERABLE = "special"  # Deprecated: use SPECIAL


class MaskingStrategy(Enum):
    """
    Column masking strategies supported by Unity Catalog.

    Each strategy defines how sensitive data should be masked
    when queried by users without appropriate privileges.
    """
    NONE = "none"  # No masking
    HASH = "hash"  # SHA-256 hashing
    REDACT = "redact"  # Full redaction (replace with XXX)
    PARTIAL = "partial"  # Partial masking (e.g., last 4 digits)
    NULLIFY = "nullify"  # Replace with NULL
    MASK_EMAIL = "mask_email"  # Email-specific masking
    MASK_POSTCODE = "mask_postcode"  # UK postcode partial masking


# Default masking strategies for each privacy classification
# Based on GDPR requirements and industry best practices
DEFAULT_MASKING_STRATEGIES = {
    PrivacyClassification.NONE: MaskingStrategy.NONE,

    # GDPR Categories
    PrivacyClassification.PII: MaskingStrategy.HASH,  # Hash for joins/analysis
    PrivacyClassification.QUASI_PII: MaskingStrategy.PARTIAL,  # Generalize/bucket
    PrivacyClassification.SPECIAL: MaskingStrategy.REDACT,  # Art. 9 - highest protection
    PrivacyClassification.CRIMINAL: MaskingStrategy.REDACT,  # Art. 10 - redact
    PrivacyClassification.CHILD: MaskingStrategy.REDACT,  # Art. 8 - protect minors

    # Industry/Risk-based
    PrivacyClassification.FINANCIAL_PII: MaskingStrategy.HASH,  # Token/encrypt recommended
    PrivacyClassification.PCI: MaskingStrategy.REDACT,  # PCI DSS - never expose
    PrivacyClassification.AUTH: MaskingStrategy.REDACT,  # Credentials - never expose
    PrivacyClassification.LOCATION: MaskingStrategy.PARTIAL,  # Generalize/round
    PrivacyClassification.TRACKING: MaskingStrategy.HASH,  # Hash for analysis
    PrivacyClassification.HR: MaskingStrategy.HASH,  # Hash identifiers

    # Non-personal but sensitive (no masking by default, use access control)
    PrivacyClassification.COMMERCIAL: MaskingStrategy.NONE,  # Access control only
    PrivacyClassification.IP: MaskingStrategy.NONE,  # Access control only
}


@dataclass
class ColumnPrivacyMetadata:
    """
    Privacy metadata for a single column from data contract.

    Extracted from contract schema.properties[].

    Attributes:
        column_name: Column name
        data_type: Column data type (string, int, etc.)
        privacy: Privacy classification (pii, quasi, vulnerable, none)
        masking_strategy: Masking strategy from contract (or 'none')
        description: Column description
        is_primary_key: Whether column is a primary key

    Example:
        ColumnPrivacyMetadata(
            column_name="POST_CODE",
            data_type="string",
            privacy=PrivacyClassification.PII,
            masking_strategy=MaskingStrategy.MASK_POSTCODE,
            description="Customer postcode",
            is_primary_key=False
        )
    """
    column_name: str
    data_type: str
    privacy: PrivacyClassification
    masking_strategy: MaskingStrategy
    description: str = ""
    is_primary_key: bool = False

    def __post_init__(self):
        """Convert string values to enums if needed."""
        if isinstance(self.privacy, str):
            self.privacy = PrivacyClassification(self.privacy)
        if isinstance(self.masking_strategy, str):
            self.masking_strategy = MaskingStrategy(self.masking_strategy)

    # Classifications where the contract maskingStrategy field can override
    # the default masking strategy. quasi_pii contains diverse data types
    # (emails, postcodes, age bands) that may need different masking functions.
    # Other classifications use their default strategy only.
    _OVERRIDE_ALLOWED_CLASSIFICATIONS = frozenset({
        PrivacyClassification.QUASI_PII,
    })

    @property
    def requires_masking(self) -> bool:
        """Check if this column requires masking based on effective strategy."""
        return (
            self.privacy != PrivacyClassification.NONE and
            self.effective_masking_strategy != MaskingStrategy.NONE
        )

    @property
    def effective_masking_strategy(self) -> MaskingStrategy:
        """
        Get effective masking strategy.

        For classifications in _OVERRIDE_ALLOWED_CLASSIFICATIONS (currently
        quasi_pii only), the contract maskingStrategy overrides the default.
        For all other classifications, the default strategy for the privacy
        classification is always used.

        If the contract maskingStrategy is 'none' (or not specified), the
        default strategy for the privacy classification is used regardless
        of classification.
        """
        # If contract specifies a strategy AND the classification allows overrides
        if (
            self.masking_strategy != MaskingStrategy.NONE
            and self.privacy in self._OVERRIDE_ALLOWED_CLASSIFICATIONS
        ):
            return self.masking_strategy

        # Use default for this privacy classification
        return DEFAULT_MASKING_STRATEGIES.get(
            self.privacy,
            MaskingStrategy.NONE
        )


@dataclass
class UCMaskingPolicy:
    """
    Unity Catalog column masking policy (actual state in UC).

    Represents what masking is currently applied in Unity Catalog.
    Retrieved by inspecting table metadata.

    Attributes:
        table: Fully qualified table name
        column_name: Column name
        masking_expression: SQL masking expression (or None if no masking)
        policy_name: Name of masking policy (if using named policies)

    Example:
        UCMaskingPolicy(
            table="catalog.schema.customer",
            column_name="email",
            masking_expression="CASE WHEN is_account_group_member('engineers') THEN email ELSE sha2(email, 256) END",
            policy_name=None
        )
    """
    table: str
    column_name: str
    masking_expression: Optional[str] = None
    policy_name: Optional[str] = None

    @property
    def has_masking(self) -> bool:
        """Check if masking is applied."""
        return self.masking_expression is not None or self.policy_name is not None


@dataclass
class MaskingIntent:
    """
    INTENDED masking state (what SHOULD exist).

    Calculated from data contract privacy metadata + role mappings.
    Represents the desired column masking configuration with role-based access.

    Attributes:
        table: Fully qualified table name
        column_name: Column name
        column_type: Column data type
        privacy: Privacy classification
        masking_strategy: Masking strategy to apply
        exempt_groups: AD groups that see unmasked data
        reason: Why this masking should exist

    Example:
        MaskingIntent(
            table="catalog.bronze_galahad.gallive_manager_address",
            column_name="POST_CODE",
            column_type="string",
            privacy=PrivacyClassification.PII,
            masking_strategy=MaskingStrategy.HASH,
            exempt_groups=["CLUK-CAZ-EDP-dev-finance-nova-data-engineer"],
            reason="PII data from contract: data.galahad.gallive_manager_address"
        )
    """
    table: str
    column_name: str
    column_type: str
    privacy: PrivacyClassification
    masking_strategy: MaskingStrategy
    exempt_groups: Set[str] = field(default_factory=set)
    reason: str = ""

    def __post_init__(self):
        """Convert string values to enums if needed."""
        if isinstance(self.privacy, str):
            self.privacy = PrivacyClassification(self.privacy)
        if isinstance(self.masking_strategy, str):
            self.masking_strategy = MaskingStrategy(self.masking_strategy)
        # Ensure exempt_groups is a set
        if isinstance(self.exempt_groups, list):
            self.exempt_groups = set(self.exempt_groups)

    def __hash__(self):
        """Make hashable for set operations."""
        return hash((self.table, self.column_name))

    def __eq__(self, other):
        """Enable equality comparison."""
        if not isinstance(other, MaskingIntent):
            return False
        return (
            self.table == other.table and
            self.column_name == other.column_name and
            self.masking_strategy == other.masking_strategy and
            self.exempt_groups == other.exempt_groups
        )


@dataclass
class MaskingDelta:
    """
    CHANGE needed (CREATE or DROP masking policy).

    Represents the difference between intended and actual masking state.
    Generated by comparing MaskingIntent vs UCMaskingPolicy.

    Attributes:
        action: "CREATE" or "DROP"
        table: Fully qualified table name
        column_name: Column name
        column_type: Column data type
        masking_strategy: Strategy to apply (for CREATE)
        exempt_groups: AD groups that see unmasked data (for CREATE)
        current_masking: Current masking expression (for DROP)
        reason: Why this change is needed

    Example:
        MaskingDelta(
            action="CREATE",
            table="catalog.bronze_galahad.gallive_manager_address",
            column_name="POST_CODE",
            column_type="string",
            masking_strategy=MaskingStrategy.HASH,
            exempt_groups={"CLUK-CAZ-EDP-dev-finance-nova-data-engineer"},
            current_masking=None,
            reason="PII requires hashing per contract"
        )
    """
    action: str  # "CREATE" or "DROP"
    table: str
    column_name: str
    column_type: str
    masking_strategy: Optional[MaskingStrategy] = None
    exempt_groups: Set[str] = field(default_factory=set)
    current_masking: Optional[str] = None
    reason: str = ""

    def __post_init__(self):
        """Validate data."""
        if self.action not in ("CREATE", "DROP"):
            raise ValueError(f"Invalid action: {self.action}. Must be CREATE or DROP")
        if isinstance(self.masking_strategy, str):
            self.masking_strategy = MaskingStrategy(self.masking_strategy)
        # Ensure exempt_groups is a set
        if isinstance(self.exempt_groups, list):
            self.exempt_groups = set(self.exempt_groups)


@dataclass
class PrivacyEnforcementResult:
    """
    Result of applying privacy/masking policies to a table.

    Tracks what was intended, what was actual, what changes were made,
    and whether those changes succeeded.

    Attributes:
        table: Fully qualified table name
        columns_with_privacy: Total columns with privacy classifications
        masking_intents: Number of masking intents calculated
        current_masked_columns: Number of currently masked columns
        policies_created: Number of masking policies created
        policies_dropped: Number of masking policies dropped
        policies_failed: Number of failed policy operations
        no_change_count: Number of columns already correctly configured
        execution_time_seconds: Time taken to execute changes
        errors: List of error messages
    """
    table: str

    # State counts
    columns_with_privacy: int
    masking_intents: int
    current_masked_columns: int
    no_change_count: int

    # Change results
    policies_created: int
    policies_dropped: int
    policies_failed: int

    # Timing and errors
    execution_time_seconds: float
    errors: List[str] = field(default_factory=list)

    @property
    def is_successful(self) -> bool:
        """Check if all changes succeeded."""
        return self.policies_failed == 0 and len(self.errors) == 0

    @property
    def total_changes(self) -> int:
        """Total number of changes attempted."""
        return self.policies_created + self.policies_dropped

    @property
    def success_rate(self) -> float:
        """Calculate success rate as percentage."""
        if self.total_changes == 0:
            return 100.0

        succeeded = self.total_changes - self.policies_failed
        return (succeeded / self.total_changes) * 100.0

    def __str__(self) -> str:
        """Human-readable string representation."""
        return (
            f"PrivacyEnforcementResult(table={self.table}, "
            f"Created={self.policies_created}, "
            f"Dropped={self.policies_dropped}, "
            f"Failed={self.policies_failed}, "
            f"Success={self.success_rate:.1f}%)"
        )
