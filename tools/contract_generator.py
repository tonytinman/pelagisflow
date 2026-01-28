# Databricks notebook source
# MAGIC %md
# MAGIC # Nova Data Contract Generator
# MAGIC
# MAGIC **Purpose**: Automatically generates data contracts for Delta tables with comprehensive privacy classification inference.
# MAGIC
# MAGIC **Features**:
# MAGIC - Supports all 13 NovaFlow sensitive data classifications (aligned with GDPR and industry standards)
# MAGIC - Automatic PII detection via regex patterns and column name heuristics
# MAGIC - Primary key inference using uniqueness and naming conventions
# MAGIC - Volume estimation through statistical sampling
# MAGIC - Default masking strategy assignment based on classification
# MAGIC
# MAGIC **Classifications Supported**:
# MAGIC | Category | Classification | Description |
# MAGIC |----------|---------------|-------------|
# MAGIC | GDPR Art. 4 | `pii` | Basic Personal Data (direct identifiers) |
# MAGIC | GDPR Art. 4 | `quasi_pii` | Quasi-Personal Data (indirect identifiers) |
# MAGIC | GDPR Art. 9 | `special` | Special Category Data (religion, health, ethnicity) |
# MAGIC | GDPR Art. 10 | `criminal` | Criminal Offence Data |
# MAGIC | GDPR Art. 8 | `child` | Children's Personal Data |
# MAGIC | Financial | `financial_pii` | Financial Personal Data (bank, salary, tax) |
# MAGIC | PCI DSS | `pci` | Payment Card Data |
# MAGIC | Security | `auth` | Authentication & Security Data |
# MAGIC | Location | `location` | Precise Location Data |
# MAGIC | Tracking | `tracking` | Online Identifiers (IP, cookies, device) |
# MAGIC | Employment | `hr` | Employment / HR Data |
# MAGIC | Commercial | `commercial` | Commercially Sensitive (non-personal) |
# MAGIC | IP | `ip` | Intellectual Property / Trade Secrets |

# COMMAND ----------

# MAGIC %md
# MAGIC ## Configuration

# COMMAND ----------

import yaml
import re
from itertools import combinations
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import Dict, List, Optional, Set, Tuple, Any

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (
    StringType, IntegerType, LongType, DoubleType, FloatType,
    DecimalType, DateType, TimestampType, ShortType, ByteType, BooleanType
)
from pyspark import StorageLevel

# Start Spark session
spark = SparkSession.builder.getOrCreate()

# COMMAND ----------

# ============================================================
#  CONFIGURATION
# ============================================================

# Source catalog and schema for reading tables
DELTA_SOURCE_CATALOG = "cluk_dev_sfsc"
DELTA_SOURCE_SCHEMA = "bronze"

# Target schema for the generated contracts
TARGET_DELTA_SCHEMA = "bronze_salesforce"

# Default ownership information
DEFAULT_DATA_OWNER = "Sam Sood"
DEFAULT_DATA_OWNER_EMAIL = "sam.sood@canadalife.co.uk"
DEFAULT_DATA_STEWARD = "Nikki Cotterell"
DEFAULT_DATA_STEWARD_EMAIL = "nikki.cotterell@canadalife.co.uk"
DEFAULT_SENIOR_MANAGER = "Sam Sood"
DEFAULT_SENIOR_MANAGER_EMAIL = "sam.sood@canadalife.co.uk"

# Default governance settings
DEFAULT_CLASSIFICATION = "Internal"
DEFAULT_DATA_RESIDENCY = "UK"
DEFAULT_DATA_PRODUCT_SUFFIX = "_raw"

# Output path for generated contracts
CONTRACT_OUTPUT_PATH = "/Volumes/cluk_dev_nova/nova_framework/data_contracts/auto/salesforce"

# Execution settings
DRY_RUN = False
MAX_THREAD_WORKERS = 4
SAMPLE_REPORT_SIZE = 10
TOP_PK_COMBO_CANDIDATES = 5

# Default table-level sampling fraction for all inference
DEFAULT_SAMPLE_FRACTION = 0.05  # 5%

# COMMAND ----------

# MAGIC %md
# MAGIC ## Privacy Classification Constants
# MAGIC
# MAGIC All 13 sensitive data classifications aligned with `nova_framework.access.privacy_models.PrivacyClassification`

# COMMAND ----------

# ============================================================
#  PRIVACY CLASSIFICATIONS (All 13 + none)
# ============================================================

# Enum values matching PrivacyClassification in nova_framework.access.privacy_models
PRIVACY_CLASSIFICATIONS = {
    "none": "No sensitivity",
    # GDPR Personal Data Categories
    "pii": "Basic Personal Data (Direct identifiers) - GDPR Art. 4",
    "quasi_pii": "Quasi-Personal Data (Indirect identifiers) - GDPR Art. 4",
    "special": "Special Category Personal Data - GDPR Art. 9",
    "criminal": "Criminal Offence Data - GDPR Art. 10",
    "child": "Children's Personal Data - GDPR Art. 8",
    # Industry/Risk-based Categories
    "financial_pii": "Financial Personal Data",
    "pci": "Payment Card Data (PCI DSS)",
    "auth": "Authentication & Security Data",
    "location": "Precise Location Data",
    "tracking": "Online Identifiers / Tracking Data",
    "hr": "Employment / HR Data",
    # Non-personal but sensitive
    "commercial": "Commercially Sensitive Financial Data",
    "ip": "Intellectual Property / Trade Secrets",
}

# Default masking strategies per classification
# Aligned with DEFAULT_MASKING_STRATEGIES in privacy_models.py
DEFAULT_MASKING_STRATEGIES = {
    "none": "none",
    "pii": "hash",
    "quasi_pii": "partial",
    "special": "redact",
    "criminal": "redact",
    "child": "redact",
    "financial_pii": "hash",
    "pci": "redact",
    "auth": "redact",
    "location": "partial",
    "tracking": "hash",
    "hr": "hash",
    "commercial": "none",  # Access control only
    "ip": "none",  # Access control only
}

# COMMAND ----------

# MAGIC %md
# MAGIC ## Regex Patterns for Data Detection
# MAGIC
# MAGIC Comprehensive regex patterns for detecting sensitive data across all 13 classifications.

# COMMAND ----------

# ============================================================
#  REGEX PATTERNS FOR DATA DETECTION
# ============================================================

# PII Detection Patterns (Basic Personal Data - Direct Identifiers)
PII_REGEX_PATTERNS = {
    "email": r"^[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Za-z]{2,}$",
    "phone_uk": r"^\+?44\s?\d{4}\s?\d{6}$",
    "phone_intl": r"^\+?\d{1,3}?[-.\s]?\(?\d{2,4}\)?[-.\s]?\d{3,4}[-.\s]?\d{3,9}$",
    "uk_postcode": r"^[A-Z]{1,2}\d[A-Z\d]?\s*\d[A-Z]{2}$",
    "nino": r"^[A-CEGHJ-PR-TW-Z]{2}\d{6}[A-D]$",
    "passport_uk": r"^\d{9}$",
    "dob_iso": r"^\d{4}-\d{2}-\d{2}$",
    "dob_uk": r"^\d{2}/\d{2}/\d{4}$",
}

# Special Category Data Patterns (GDPR Art. 9)
SPECIAL_REGEX_PATTERNS = {
    "religion_indicator": r"(?i)(muslim|christian|jewish|hindu|sikh|buddhist|atheist)",
    "ethnicity_code": r"^[A-Z]{1,2}\d{1,2}$",  # UK ethnicity codes
    "health_code_icd": r"^[A-Z]\d{2}(\.\d{1,2})?$",  # ICD-10 codes
}

# Financial PII Patterns
FINANCIAL_REGEX_PATTERNS = {
    "uk_sort_code": r"^\d{2}-\d{2}-\d{2}$",
    "uk_account": r"^\d{8}$",
    "iban": r"^[A-Z]{2}\d{2}[A-Z0-9]{1,30}$",
    "bic_swift": r"^[A-Z]{4}[A-Z]{2}[A-Z0-9]{2}([A-Z0-9]{3})?$",
    "salary_amount": r"^\d{4,6}(\.\d{2})?$",  # Typical salary range
}

# PCI DSS Patterns (Payment Card Data)
PCI_REGEX_PATTERNS = {
    "credit_card": r"^(?:\d[ -]*?){13,16}$",
    "credit_card_no_space": r"^\d{13,16}$",
    "cvv": r"^\d{3,4}$",
    "card_expiry": r"^(0[1-9]|1[0-2])/?([0-9]{2}|[0-9]{4})$",
}

# Authentication & Security Patterns
AUTH_REGEX_PATTERNS = {
    "api_key": r"^[A-Za-z0-9_-]{20,64}$",
    "jwt_token": r"^eyJ[A-Za-z0-9_-]+\.eyJ[A-Za-z0-9_-]+\.[A-Za-z0-9_-]+$",
    "password_hash_bcrypt": r"^\$2[aby]?\$\d{1,2}\$[./A-Za-z0-9]{53}$",
    "password_hash_sha": r"^[a-f0-9]{64}$",  # SHA-256
}

# Location Patterns
LOCATION_REGEX_PATTERNS = {
    "gps_coord": r"^-?\d{1,3}\.\d{4,8}$",
    "lat_long_pair": r"^-?\d{1,3}\.\d+,\s*-?\d{1,3}\.\d+$",
    "uk_postcode_full": r"^[A-Z]{1,2}\d[A-Z\d]?\s*\d[A-Z]{2}$",
}

# Tracking/Online Identifier Patterns
TRACKING_REGEX_PATTERNS = {
    "ipv4": r"^(\d{1,3}\.){3}\d{1,3}$",
    "ipv6": r"^([0-9a-fA-F]{1,4}:){7}[0-9a-fA-F]{1,4}$",
    "mac_address": r"^([0-9A-Fa-f]{2}[:-]){5}([0-9A-Fa-f]{2})$",
    "uuid": r"^[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}$",
    "device_id": r"^[A-Za-z0-9_-]{16,64}$",
}

# Criminal Record Patterns (simplified - typically codes/references)
CRIMINAL_REGEX_PATTERNS = {
    "pnc_number": r"^\d{2}/\d+[A-Z]$",  # UK Police National Computer
    "offence_code": r"^[A-Z]{2,4}\d{2,4}$",
}

# COMMAND ----------

# MAGIC %md
# MAGIC ## Name-Based Detection Indicators
# MAGIC
# MAGIC Column name patterns for each classification category.

# COMMAND ----------

# ============================================================
#  NAME-BASED INDICATORS (Column Name Heuristics)
# ============================================================

# PII - Basic Personal Data (Direct Identifiers)
NAME_BASED_PII = [
    "name", "first_name", "last_name", "surname", "given_name", "middle_name",
    "full_name", "email", "phone", "mobile", "telephone", "fax",
    "address", "street", "postcode", "zip", "zipcode", "postal",
    "dob", "date_of_birth", "birth_date", "birthdate",
    "nino", "national_insurance", "ni_number",
    "passport", "passport_number", "driving_license", "drivers_license",
    "ssn", "social_security", "national_id",
]

# Quasi-PII - Indirect Identifiers
NAME_BASED_QUASI_PII = [
    "customer_id", "client_id", "user_id", "employee_id", "member_id",
    "tenant_id", "policy_id", "account_id", "account_no", "account_number",
    "claim_id", "case_id", "reference_id", "ref_id",
    "hash", "hash_key", "surrogate_key", "skey",
    "segment_id", "group_id", "region", "district",
    "age_band", "age_group", "gender", "sex",
]

# Special Category Data (GDPR Art. 9)
NAME_BASED_SPECIAL = [
    "health", "medical", "diagnosis", "condition", "symptom",
    "disability", "disabled", "impairment",
    "religion", "religious", "faith", "belief",
    "ethnicity", "ethnic", "race", "racial", "nationality",
    "political", "party", "union", "trade_union",
    "sexual", "orientation", "sex_life",
    "genetic", "dna", "genome",
    "biometric", "fingerprint", "retina", "face_id",
]

# Criminal Offence Data (GDPR Art. 10)
NAME_BASED_CRIMINAL = [
    "criminal", "offence", "offense", "conviction", "sentence",
    "caution", "arrest", "charge", "prosecution",
    "prison", "probation", "parole",
    "pnc", "dbs", "crb", "police_check",
]

# Children's Data (GDPR Art. 8)
NAME_BASED_CHILD = [
    "child", "minor", "juvenile", "youth",
    "parent", "guardian", "parental_consent",
    "school", "pupil", "student_under",
    "age_verified", "is_minor", "is_child",
]

# Financial PII
NAME_BASED_FINANCIAL_PII = [
    "bank_account", "account_number", "sort_code", "routing",
    "iban", "swift", "bic",
    "salary", "wage", "income", "earnings", "compensation",
    "tax_id", "tax_number", "vat", "paye",
    "credit_score", "credit_rating",
    "loan", "mortgage", "debt",
]

# PCI DSS - Payment Card Data
NAME_BASED_PCI = [
    "card_number", "credit_card", "debit_card", "pan",
    "cvv", "cvc", "cvv2", "security_code",
    "card_expiry", "expiry_date", "expiration",
    "cardholder", "card_holder",
]

# Authentication & Security
NAME_BASED_AUTH = [
    "password", "passwd", "pwd", "secret",
    "api_key", "apikey", "access_key", "secret_key",
    "token", "auth_token", "refresh_token", "bearer",
    "private_key", "encryption_key", "signing_key",
    "credential", "certificate", "cert",
    "mfa", "otp", "totp", "2fa",
]

# Location Data
NAME_BASED_LOCATION = [
    "latitude", "longitude", "lat", "lng", "lon",
    "gps", "coordinates", "coord", "geolocation",
    "home_address", "work_address", "location",
    "ip_location", "geo_ip",
]

# Tracking / Online Identifiers
NAME_BASED_TRACKING = [
    "ip_address", "ip_addr", "client_ip", "remote_ip",
    "device_id", "device_fingerprint", "browser_fingerprint",
    "cookie_id", "session_id", "tracking_id", "visitor_id",
    "mac_address", "imei", "imsi",
    "advertising_id", "idfa", "gaid",
    "user_agent", "browser_id",
]

# HR / Employment Data
NAME_BASED_HR = [
    "performance", "rating", "review", "appraisal",
    "disciplinary", "grievance", "complaint",
    "sick_leave", "absence", "leave_reason",
    "termination", "redundancy", "dismissal",
    "payroll", "bonus", "commission",
    "contract_type", "employment_status",
    "job_title", "department", "manager",
]

# Commercial Sensitive (Non-Personal)
NAME_BASED_COMMERCIAL = [
    "revenue", "profit", "margin", "ebitda",
    "forecast", "projection", "budget",
    "pricing", "cost", "markup",
    "market_share", "competitor",
    "acquisition", "merger", "deal",
    "confidential", "restricted",
]

# Intellectual Property (Non-Personal)
NAME_BASED_IP = [
    "algorithm", "model_weights", "model_params",
    "trade_secret", "proprietary",
    "patent", "copyright", "trademark",
    "source_code", "codebase",
    "formula", "recipe", "process",
    "design", "blueprint", "schematic",
]

# COMMAND ----------

# MAGIC %md
# MAGIC ## Privacy Inference Engine
# MAGIC
# MAGIC Core logic for classifying columns into one of the 13 sensitivity categories.

# COMMAND ----------

# ============================================================
#  PRIVACY INFERENCE ENGINE
# ============================================================

def infer_privacy_from_data(df) -> Dict[str, str]:
    """
    Classifies each column into one of the 14 classifications (13 sensitive + none).

    Classification hierarchy (highest to lowest priority):
    1. special (Art. 9) - Highest protection
    2. criminal (Art. 10)
    3. child (Art. 8)
    4. pci - Never expose card data
    5. auth - Credentials never exposed
    6. pii - Direct identifiers
    7. financial_pii
    8. location
    9. hr
    10. tracking
    11. quasi_pii
    12. commercial
    13. ip
    14. none

    Returns:
        Dict[str, str]: Mapping of column name to classification
    """

    privacy_results = {}

    for col in df.columns:
        cname = col.lower()
        detected: Set[str] = set()

        # ========================================
        # 1. Name-based heuristics (all categories)
        # ========================================

        # Check each category
        if any(t in cname for t in NAME_BASED_SPECIAL):
            detected.add("special")
        if any(t in cname for t in NAME_BASED_CRIMINAL):
            detected.add("criminal")
        if any(t in cname for t in NAME_BASED_CHILD):
            detected.add("child")
        if any(t in cname for t in NAME_BASED_PCI):
            detected.add("pci")
        if any(t in cname for t in NAME_BASED_AUTH):
            detected.add("auth")
        if any(t in cname for t in NAME_BASED_PII):
            detected.add("pii")
        if any(t in cname for t in NAME_BASED_FINANCIAL_PII):
            detected.add("financial_pii")
        if any(t in cname for t in NAME_BASED_LOCATION):
            detected.add("location")
        if any(t in cname for t in NAME_BASED_HR):
            detected.add("hr")
        if any(t in cname for t in NAME_BASED_TRACKING):
            detected.add("tracking")
        if any(t in cname for t in NAME_BASED_QUASI_PII):
            detected.add("quasi_pii")
        if any(t in cname for t in NAME_BASED_COMMERCIAL):
            detected.add("commercial")
        if any(t in cname for t in NAME_BASED_IP):
            detected.add("ip")

        # ========================================
        # 2. Regex-based data detection
        # ========================================

        col_str = F.col(col).cast("string")

        # Check PII patterns
        for label, pattern in PII_REGEX_PATTERNS.items():
            try:
                matched = df.select(F.max(col_str.rlike(pattern))).first()[0]
                if matched:
                    detected.add("pii")
                    break  # Found PII, no need to check more PII patterns
            except Exception:
                pass

        # Check Special Category patterns
        for label, pattern in SPECIAL_REGEX_PATTERNS.items():
            try:
                matched = df.select(F.max(col_str.rlike(pattern))).first()[0]
                if matched:
                    detected.add("special")
                    break
            except Exception:
                pass

        # Check Financial patterns
        for label, pattern in FINANCIAL_REGEX_PATTERNS.items():
            try:
                matched = df.select(F.max(col_str.rlike(pattern))).first()[0]
                if matched:
                    detected.add("financial_pii")
                    break
            except Exception:
                pass

        # Check PCI patterns
        for label, pattern in PCI_REGEX_PATTERNS.items():
            try:
                matched = df.select(F.max(col_str.rlike(pattern))).first()[0]
                if matched:
                    detected.add("pci")
                    break
            except Exception:
                pass

        # Check Auth patterns
        for label, pattern in AUTH_REGEX_PATTERNS.items():
            try:
                matched = df.select(F.max(col_str.rlike(pattern))).first()[0]
                if matched:
                    detected.add("auth")
                    break
            except Exception:
                pass

        # Check Location patterns
        for label, pattern in LOCATION_REGEX_PATTERNS.items():
            try:
                matched = df.select(F.max(col_str.rlike(pattern))).first()[0]
                if matched:
                    detected.add("location")
                    break
            except Exception:
                pass

        # Check Tracking patterns
        for label, pattern in TRACKING_REGEX_PATTERNS.items():
            try:
                matched = df.select(F.max(col_str.rlike(pattern))).first()[0]
                if matched:
                    detected.add("tracking")
                    break
            except Exception:
                pass

        # Check Criminal patterns
        for label, pattern in CRIMINAL_REGEX_PATTERNS.items():
            try:
                matched = df.select(F.max(col_str.rlike(pattern))).first()[0]
                if matched:
                    detected.add("criminal")
                    break
            except Exception:
                pass

        # ========================================
        # 3. Precedence resolution (highest protection wins)
        # ========================================

        if "special" in detected:
            privacy_results[col] = "special"
        elif "criminal" in detected:
            privacy_results[col] = "criminal"
        elif "child" in detected:
            privacy_results[col] = "child"
        elif "pci" in detected:
            privacy_results[col] = "pci"
        elif "auth" in detected:
            privacy_results[col] = "auth"
        elif "pii" in detected:
            privacy_results[col] = "pii"
        elif "financial_pii" in detected:
            privacy_results[col] = "financial_pii"
        elif "location" in detected:
            privacy_results[col] = "location"
        elif "hr" in detected:
            privacy_results[col] = "hr"
        elif "tracking" in detected:
            privacy_results[col] = "tracking"
        elif "quasi_pii" in detected:
            privacy_results[col] = "quasi_pii"
        elif "commercial" in detected:
            privacy_results[col] = "commercial"
        elif "ip" in detected:
            privacy_results[col] = "ip"
        else:
            privacy_results[col] = "none"

    return privacy_results


def get_default_masking_strategy(privacy_classification: str) -> str:
    """
    Returns the default masking strategy for a given privacy classification.

    Args:
        privacy_classification: One of the 14 classification values

    Returns:
        Masking strategy string (hash, redact, partial, none, etc.)
    """
    return DEFAULT_MASKING_STRATEGIES.get(privacy_classification, "none")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Volume Classification

# COMMAND ----------

# ============================================================
#  Row Count -> Volume Mapping
# ============================================================

VOLUME_ROW_RANGES = {
    "XXS": (0, 10_000),
    "XS": (10_001, 100_000),
    "S": (100_001, 1_000_000),
    "M": (1_000_001, 10_000_000),
    "L": (10_000_001, 100_000_000),
    "XL": (100_000_001, 1_000_000_000),
    "XXL": (1_000_000_001, 10_000_000_000),
    "XXXL": (10_000_000_001, float("inf")),
}


def map_row_count_to_volume(rc: int) -> str:
    """Map row count to volume classification."""
    for size, (low, high) in VOLUME_ROW_RANGES.items():
        if low <= rc <= high:
            return size
    return "M"

# COMMAND ----------

# MAGIC %md
# MAGIC ## Primary Key Inference

# COMMAND ----------

# ============================================================
#  PRIMARY KEY INFERENCE
# ============================================================

PK_HINTS = ["id", "_id", "code", "_code", "pk", "uuid", "guid", "hash"]

PK_ALLOWED_TYPES = (
    StringType, IntegerType, LongType, DoubleType, FloatType,
    DecimalType, DateType, TimestampType, ShortType, ByteType, BooleanType
)


def pk_name_score(col: str) -> int:
    """
    Scoring heuristic based on column name strength.
    """
    c = col.lower()
    score = 0
    if c == "id":
        score += 5
    if c.endswith("_id"):
        score += 5
    if any(h in c for h in PK_HINTS):
        score += 3
    return score


def collect_column_stats(df, columns: List[str]) -> Dict[str, Dict[str, int]]:
    """
    Collect null counts and approx distinct counts for candidate PK columns.
    Uses the already sampled dataframe.
    """
    if not columns:
        return {}

    agg_exprs = []
    aliases = {}

    for idx, col in enumerate(columns):
        distinct_alias = f"distinct_{idx}"
        null_alias = f"null_{idx}"

        agg_exprs.append(F.approx_count_distinct(F.col(col)).alias(distinct_alias))
        agg_exprs.append(F.sum(F.when(F.col(col).isNull(), 1).otherwise(0)).alias(null_alias))

        aliases[col] = (distinct_alias, null_alias)

    result = df.agg(*agg_exprs).collect()[0]
    stats = {}

    for col, (distinct_a, null_a) in aliases.items():
        stats[col] = {
            "distinct": result[distinct_a] or 0,
            "nulls": result[null_a] or 0
        }

    return stats


def infer_primary_key(
    df,
    sample_row_count: int,
    candidate_columns: List[str],
    stats: Dict[str, Dict[str, int]]
) -> List[str]:
    """
    Attempts to infer:
       - Single-column PK
       - Composite PKs (pairs)
    Uses sampled row counts.
    """

    if sample_row_count == 0 or not candidate_columns:
        return []

    ranked = []

    # 1 - Score columns based on name, distinct ratio, null ratio
    for col in candidate_columns:
        s = stats[col]
        dr = (s["distinct"] / sample_row_count) if sample_row_count else 0
        nr = (s["nulls"] / sample_row_count) if sample_row_count else 0
        score = pk_name_score(col) * 10 + dr * 50 - nr * 30
        ranked.append((col, score, dr, nr))

    ranked.sort(key=lambda x: x[1], reverse=True)

    if not ranked:
        return []

    # 2 - Check best single-column PK
    best_col, best_score, best_dr, _ = ranked[0]

    if best_score > 60 and best_dr > 0.98:
        return [best_col]

    # 3 - Composite PK inference
    if len(ranked) > 1:
        top_cols = [col for col, *_ in ranked[:TOP_PK_COMBO_CANDIDATES]]
        combos = list(combinations(top_cols, 2))

        exprs = [
            F.approx_count_distinct(F.struct(F.col(a), F.col(b))).alias(f"combo_{i}")
            for i, (a, b) in enumerate(combos)
        ]

        results = df.agg(*exprs).collect()[0].asDict()

        for i, (a, b) in enumerate(combos):
            approx_d = results[f"combo_{i}"] or 0
            if approx_d / sample_row_count > 0.98:
                return [a, b]

    return []

# COMMAND ----------

# MAGIC %md
# MAGIC ## Schema Metadata Extraction

# COMMAND ----------

# ============================================================
#  SCHEMA METADATA EXTRACTION
# ============================================================

def extract_schema_metadata(
    df,
    inferred_privacy: Dict[str, str]
) -> Tuple[List[Dict[str, Any]], List[str]]:
    """
    Extracts column metadata using already-inferred privacy classifications.
    PK candidates collected here.

    Returns:
        Tuple of (column properties list, PK candidate column names)
    """
    props = []
    pk_candidates = []

    for field in df.schema.fields:
        if field.name == "ingestdatetime":
            continue

        privacy_class = inferred_privacy.get(field.name, "none")

        col_info = {
            "name": field.name,
            "type": field.dataType.simpleString(),
            "nullable": field.nullable,
            "description": "",
            "privacy": privacy_class,
            "maskingStrategy": get_default_masking_strategy(privacy_class),
            "isPrimaryKey": False,
            "isChangeTracking": False,
            "tags": []
        }
        props.append(col_info)

        if isinstance(field.dataType, PK_ALLOWED_TYPES):
            pk_candidates.append(field.name)

    return props, pk_candidates

# COMMAND ----------

# MAGIC %md
# MAGIC ## Contract Builder
# MAGIC
# MAGIC Core function that builds a complete data contract for a table.

# COMMAND ----------

# ============================================================
#  CONTRACT BUILDER (Sampling, PK, Privacy)
# ============================================================

def build_contract(domain: str, table: str, sample_fraction: float) -> Dict[str, Any]:
    """
    Builds a single contract for a given table using:
      - table-level sampling
      - sampled row count
      - estimated full row count
      - sampled PK inference
      - sampled privacy inference (all 13 classifications)

    Args:
        domain: Domain name for the contract
        table: Table name
        sample_fraction: Fraction of data to sample (0.0-1.0)

    Returns:
        Complete contract dictionary
    """

    full_name = f"{DELTA_SOURCE_CATALOG}.{DELTA_SOURCE_SCHEMA}.{table}"

    # ========================================================
    # 1 - SAMPLE THE TABLE AT READ TIME (CRITICAL)
    # ========================================================
    df = (
        spark.table(full_name)
            .sample(withReplacement=False, fraction=sample_fraction, seed=42)
            .persist(StorageLevel.MEMORY_AND_DISK)
    )

    try:
        # 2 - Row counts
        sample_row_count = df.count()

        # Fallback: re-sample at 100% ONLY if sample returned zero rows
        if sample_row_count == 0:
            df.unpersist()
            df = (
                spark.table(full_name)
                    .sample(withReplacement=False, fraction=1.0, seed=42)
                    .persist(StorageLevel.MEMORY_AND_DISK)
            )
            sample_row_count = df.count()
            estimated_row_count = sample_row_count
        else:
            estimated_row_count = int(sample_row_count / sample_fraction)

        # 3 - Volume classification (on estimated row count)
        volume = map_row_count_to_volume(estimated_row_count)

        # 4 - Privacy inference (works on sampled df - all 13 classifications)
        inferred_privacy = infer_privacy_from_data(df)

        # 5 - Extract schema metadata + PK candidates
        columns, pk_candidates = extract_schema_metadata(df, inferred_privacy)
        print(f"{full_name}: PK candidates = {pk_candidates}")

        # 6 - PK inference (sample-based)
        col_stats = collect_column_stats(df, pk_candidates)
        inferred_pks = infer_primary_key(
            df,
            sample_row_count,
            pk_candidates,
            col_stats
        )
        no_pk_identified = len(inferred_pks) == 0

        # Mark columns that are PKs
        for c in columns:
            c["isPrimaryKey"] = c["name"] in inferred_pks
            c["isChangeTracking"] = c["name"] not in inferred_pks

        # ========================================================
        # 7 - Privacy summary statistics
        # ========================================================
        privacy_summary = {}
        for c in columns:
            priv = c["privacy"]
            privacy_summary[priv] = privacy_summary.get(priv, 0) + 1

        # Determine if table contains any personal data
        personal_data_classifications = {
            "pii", "quasi_pii", "special", "criminal", "child",
            "financial_pii", "pci", "auth", "location", "tracking", "hr"
        }
        contains_personal_data = any(
            c["privacy"] in personal_data_classifications for c in columns
        )

        # Check for special category data specifically
        contains_special_category = any(
            c["privacy"] in {"special", "criminal", "child"} for c in columns
        )

        # ========================================================
        # 8 - Build contract JSON structure
        # ========================================================
        contract_name = f"data.{domain}.{table}"

        contract = {
            "apiVersion": "v3.0.2",
            "kind": "DataContract",
            "name": contract_name,
            "version": "1.0.0",
            "domain": domain,
            "dataProduct": f"{domain}{DEFAULT_DATA_PRODUCT_SUFFIX}",

            "team": [
                {"role": "dataOwner", "name": DEFAULT_DATA_OWNER, "email": DEFAULT_DATA_OWNER_EMAIL},
                {"role": "seniorManager", "name": DEFAULT_SENIOR_MANAGER, "email": DEFAULT_SENIOR_MANAGER_EMAIL},
                {"role": "dataSteward", "name": DEFAULT_DATA_STEWARD, "email": DEFAULT_DATA_STEWARD_EMAIL},
            ],

            "schema": {
                "name": TARGET_DELTA_SCHEMA,
                "type": "table",
                "table": table,
                "format": "delta",
                "description": f"Auto-generated contract for {table}",
                "tags": [],
                "properties": columns
            },

            "customProperties": {
                "sourceFormat": "delta",

                # Row counts and volume
                "rowCount": estimated_row_count,
                "volume": volume,
                "inferredPrimaryKeys": inferred_pks,

                # Privacy summary
                "privacySummary": privacy_summary,

                "governance": {
                    "classification": DEFAULT_CLASSIFICATION,
                    "containsPersonalData": contains_personal_data,
                    "containsSpecialCategoryData": contains_special_category,
                    "specialCategoryData": contains_special_category,  # Deprecated alias
                    "dataResidency": DEFAULT_DATA_RESIDENCY,
                    "retention_period_days": 3650,
                    "retention_rationale": "UK GDPR Data"
                },

                "accessControl": {
                    "read": [],
                    "readSensitive": [],
                    "write": []
                }
            },

            # Include sample row count for internal reporting only
            "_internal": {
                "sample_row_count": sample_row_count,
                "sample_fraction": sample_fraction,
                "no_pk_identified": no_pk_identified
            }
        }

        return contract

    finally:
        df.unpersist()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Contract Writer

# COMMAND ----------

# ============================================================
#  CONTRACT WRITER
# ============================================================

def write_contract(contract: Dict[str, Any]) -> None:
    """
    Writes the contract YAML file unless DRY_RUN is enabled.
    """

    table = contract["schema"]["table"]

    if DRY_RUN:
        print(f"\n--- DRY RUN: {table} ---")
        print(f"Estimated row count: {contract['customProperties']['rowCount']}")
        print(f"Sample row count:    {contract['_internal']['sample_row_count']}")
        print(f"Privacy summary:     {contract['customProperties']['privacySummary']}")
        print("Columns:")
        for c in contract["schema"]["properties"]:
            print(f"  {c['name']} -> privacy={c['privacy']}, masking={c['maskingStrategy']}, pk={c['isPrimaryKey']}")
        print("--- END DRY RUN ---\n")
        return

    path = f"{CONTRACT_OUTPUT_PATH}/data.{contract['domain']}.{table}.yaml"

    # Remove _internal section before writing
    contract_to_write = {k: v for k, v in contract.items() if k != "_internal"}

    with open(path, "w") as f:
        yaml.dump(contract_to_write, f, sort_keys=False, default_flow_style=False)

    print(f"Contract written: {path}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Parallel Table Execution

# COMMAND ----------

# ============================================================
#  PARALLEL TABLE EXECUTION
# ============================================================

def analyze_tables_parallel(
    domain: str,
    tables: List[str],
    sample_fraction: float
) -> List[Dict[str, Any]]:
    """
    Executes contract generation across multiple tables in parallel.
    Each table uses the same sampling fraction.
    """
    results = []

    with ThreadPoolExecutor(max_workers=MAX_THREAD_WORKERS) as executor:
        futures = {
            executor.submit(build_contract, domain, table, sample_fraction): table
            for table in tables
        }

        for future in as_completed(futures):
            table = futures[future]
            try:
                contract = future.result()
                results.append({"table": table, "status": "ok", "contract": contract})
            except Exception as exc:
                results.append({"table": table, "status": "error", "error": str(exc)})
                print(f"Contract failed for {table}: {exc}")

    # Sort results for consistent reporting
    results.sort(key=lambda x: x["table"])
    return results

# COMMAND ----------

# MAGIC %md
# MAGIC ## Sample Report

# COMMAND ----------

# ============================================================
#  SAMPLE REPORT
# ============================================================

def print_sample_report(results: List[Dict[str, Any]], sample_size: int = SAMPLE_REPORT_SIZE) -> None:
    """
    Displays a report of the first N successfully processed tables.
    Includes sample_row_count, estimated_row_count, and privacy summary.
    """

    successful = [r for r in results if r["status"] == "ok"]
    if not successful:
        print("\n=== SAMPLE REPORT ===")
        print("No successfully processed tables.")
        print("=====================\n")
        return

    sample = successful[:min(sample_size, len(successful))]

    print(f"\n=== SAMPLE REPORT ({len(sample)} of {len(successful)} tables) ===")
    total_estimated = 0

    for entry in sample:
        contract = entry["contract"]
        table = contract["schema"]["table"]

        sample_count = contract["_internal"]["sample_row_count"]
        estimated_count = contract["customProperties"]["rowCount"]
        volume = contract["customProperties"]["volume"]
        pks = contract["customProperties"]["inferredPrimaryKeys"] or "n/a"
        privacy_summary = contract["customProperties"]["privacySummary"]

        # Count sensitive columns
        sensitive_count = sum(
            v for k, v in privacy_summary.items()
            if k != "none"
        )

        total_estimated += estimated_count

        print(
            f"* {table}\n"
            f"    sample_rows={sample_count}, estimated_rows={estimated_count}, volume={volume}\n"
            f"    columns={len(contract['schema']['properties'])}, sensitive_cols={sensitive_count}\n"
            f"    pk={pks}\n"
            f"    privacy: {privacy_summary}"
        )

    print(f"\nTotal estimated rows (sampled tables): {total_estimated:,}")
    print("=== END SAMPLE REPORT ===\n")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Table Enumeration

# COMMAND ----------

# ============================================================
#  TABLE ENUMERATION
# ============================================================

def get_all_tables(catalog: str, schema: str) -> List[str]:
    """
    Returns all tables in the given UC catalog.schema.
    """
    df = spark.sql(f"SHOW TABLES IN {catalog}.{schema}")
    return [row.tableName for row in df.collect()]


def filter_tables(all_tables: List[str], table_filter) -> List[str]:
    """
    Supports:
        - Exact table name
        - Wildcards: *, %
        - List of table names
        - None = all tables
    """
    if table_filter is None:
        return all_tables

    # Literal name
    if isinstance(table_filter, str) and not any(c in table_filter for c in "*%"):
        return [t for t in all_tables if t == table_filter]

    # Wildcard filter
    if isinstance(table_filter, str):
        regex = "^" + table_filter.replace("*", ".*").replace("%", ".*") + "$"
        return [t for t in all_tables if re.match(regex, t)]

    # List of names
    if isinstance(table_filter, list):
        requested = set(table_filter)
        return [t for t in all_tables if t in requested]

    raise ValueError("Invalid table_filter parameter")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Entry Points
# MAGIC
# MAGIC Main functions for generating contracts.

# COMMAND ----------

# ============================================================
#  ENTRYPOINT: CONTRACTS FOR SPECIFIC TABLES
# ============================================================

def generate_contracts_for_tables(
    domain: str,
    tables: List[str],
    sample_fraction: float = DEFAULT_SAMPLE_FRACTION,
    sample_report_size: int = SAMPLE_REPORT_SIZE
) -> None:
    """
    Generates contracts for a fixed list of tables.

    Args:
        domain: Domain name for contracts
        tables: List of table names to process
        sample_fraction: Fraction of data to sample (default 5%)
        sample_report_size: Number of tables to show in report
    """
    tables = sorted(set(tables))

    if not tables:
        print("No tables to process.")
        return

    mode = "DRY RUN" if DRY_RUN else "WRITE MODE"
    print(f"\n=== {mode} | {len(tables)} tables | sample_fraction={sample_fraction} ===")

    print(f"Tables to analyze: {tables}")
    results = analyze_tables_parallel(domain, tables, sample_fraction)
    print_sample_report(results, sample_report_size)

    # Write contracts
    for entry in results:
        if entry["status"] == "ok":
            write_contract(entry["contract"])
        else:
            print(f"Skipping write for {entry['table']} due to errors.")

# COMMAND ----------

# ============================================================
#  ENTRYPOINT: CONTRACTS FOR AN ENTIRE SCHEMA
# ============================================================

def generate_contracts_for_schema(
    domain: str,
    table_filter=None,
    sample_fraction: float = DEFAULT_SAMPLE_FRACTION,
    sample_report_size: int = SAMPLE_REPORT_SIZE
) -> None:
    """
    Generates contracts for all or filtered tables in a schema.

    Args:
        domain: Domain name for contracts
        table_filter: Filter for tables (None=all, string with wildcards, or list)
        sample_fraction: Fraction of data to sample (default 5%)
        sample_report_size: Number of tables to show in report (0=all)
    """
    all_tables = get_all_tables(DELTA_SOURCE_CATALOG, DELTA_SOURCE_SCHEMA)
    tables = filter_tables(all_tables, table_filter)

    if sample_report_size == 0:
        sample_report_size = len(tables)

    if not tables:
        print("No matching tables found.")
        return

    mode = "DRY RUN" if DRY_RUN else "WRITE MODE"
    print(f"\n=== {mode} | schema={DELTA_SOURCE_SCHEMA} | sample_fraction={sample_fraction} ===")
    print(f"Tables included: {tables}")

    results = analyze_tables_parallel(domain, tables, sample_fraction)
    print_sample_report(results, sample_report_size)

    # Write contracts
    for entry in results:
        if entry["status"] == "ok":
            write_contract(entry["contract"])
        else:
            print(f"Skipping write for {entry['table']} due to errors.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Usage Examples
# MAGIC
# MAGIC ```python
# MAGIC # Generate contracts for specific tables
# MAGIC generate_contracts_for_tables(
# MAGIC     domain="salesforce",
# MAGIC     tables=["account", "contact", "opportunity"],
# MAGIC     sample_fraction=0.05
# MAGIC )
# MAGIC
# MAGIC # Generate contracts for all tables matching a pattern
# MAGIC generate_contracts_for_schema(
# MAGIC     domain="salesforce",
# MAGIC     table_filter="account*",
# MAGIC     sample_fraction=0.10
# MAGIC )
# MAGIC
# MAGIC # Generate contracts for all tables in the schema
# MAGIC generate_contracts_for_schema(
# MAGIC     domain="salesforce",
# MAGIC     table_filter=None,
# MAGIC     sample_fraction=0.05
# MAGIC )
# MAGIC ```
# MAGIC
# MAGIC ## Privacy Classifications Reference
# MAGIC
# MAGIC | Classification | GDPR Article | Default Masking | Example Fields |
# MAGIC |----------------|--------------|-----------------|----------------|
# MAGIC | `pii` | Art. 4 | hash | email, name, phone |
# MAGIC | `quasi_pii` | Art. 4 | partial | customer_id, age_band |
# MAGIC | `special` | Art. 9 | redact | health, religion, ethnicity |
# MAGIC | `criminal` | Art. 10 | redact | conviction, offence |
# MAGIC | `child` | Art. 8 | redact | minor, parental_consent |
# MAGIC | `financial_pii` | - | hash | bank_account, salary |
# MAGIC | `pci` | PCI DSS | redact | card_number, cvv |
# MAGIC | `auth` | Art. 32 | redact | password, api_key |
# MAGIC | `location` | - | partial | latitude, gps |
# MAGIC | `tracking` | - | hash | ip_address, device_id |
# MAGIC | `hr` | - | hash | performance, disciplinary |
# MAGIC | `commercial` | - | none | revenue, forecast |
# MAGIC | `ip` | - | none | algorithm, trade_secret |
