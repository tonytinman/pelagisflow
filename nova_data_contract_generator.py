# ============================================================
#  NOVA DATA CONTRACT GENERATOR)
#  Includes:
#     - Imports
#     - Spark session
#     - Core config
#     - Sampling parameters
#     - Privacy inference (PII / quasi-PII / vulnerable)
# ============================================================

import yaml
import re
from itertools import combinations
from concurrent.futures import ThreadPoolExecutor, as_completed

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (
    StringType, IntegerType, LongType, DoubleType, FloatType,
    DecimalType, DateType, TimestampType, ShortType, ByteType, BooleanType
)
from pyspark import StorageLevel

# Start Spark session
spark = SparkSession.builder.getOrCreate()

# ============================================================
#  CONFIGURATION
# ============================================================

DELTA_SOURCE_CATALOG = "cluk_dev_galahad"
DELTA_SOURCE_SCHEMA  = "bronze"

TARGET_DELTA_SCHEMA  = "bronze_galahad"

DEFAULT_DATA_OWNER           = "Sam Sood"
DEFAULT_DATA_OWNER_EMAIL     = "sam.sood@canadalife.co.uk"
DEFAULT_DATA_STEWARD         = "Nikki Cotterell"
DEFAULT_DATA_STEWARD_EMAIL   = "nikki.cotterell@canadalife.co.uk"
DEFAULT_SENIOR_MANAGER       = "Sam Sood"
DEFAULT_SENIOR_MANAGER_EMAIL = "sam.sood@canadalife.co.uk"

DEFAULT_CLASSIFICATION = "Internal"
DEFAULT_DATA_RESIDENCY = "UK"
DEFAULT_DATA_PRODUCT_SUFFIX = "_raw"

CONTRACT_OUTPUT_PATH = "/Volumes/cluk_dev_nova/nova_framework/data_contracts/galahad"

DRY_RUN = False
MAX_THREAD_WORKERS = 2  # Reduced from 4 to prevent socket exhaustion
SAMPLE_REPORT_SIZE = 10
TOP_PK_COMBO_CANDIDATES = 5

# Default table-level sampling fraction for all inference
DEFAULT_SAMPLE_FRACTION = 0.10   # 10%

# ============================================================
#  PRIVACY INFERENCE
# ============================================================

# Regex patterns for PII detection
PII_REGEX_PATTERNS = {
    "email":        r"^[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Za-z]{2,}$",
    "phone":        r"^\+?\d{1,3}?[-.\s]?\(?\d{2,4}\)?[-.\s]?\d{3,4}[-.\s]?\d{3,9}$",
    "uk_postcode":  r"^[A-Z]{1,2}\d[A-Z\d]?\s*\d[A-Z]{2}$",
    "ip":           r"^(\d{1,3}\.){3}\d{1,3}$",
    "nino":         r"^[A-CEGHJ-PR-TW-Z]{2}\d{6}[A-D]$",
    "dob":          r"^\d{4}-\d{2}-\d{2}$",
    "credit_card":  r"^(?:\d[ -]*?){13,16}$",
}

# Name-based PII indicators
NAME_BASED_PII = [
    "name", "first", "last", "surname", "email", "phone", "mobile",
    "address", "postcode", "dob", "birth", "nino", "passport",
    "ip", "ssn", "national"
]

# Name-based quasi-PII indicators
NAME_BASED_QUASI = [
    "customer_id", "client_id", "user_id", "employee_id",
    "member_id", "tenant_id", "policy_id", "account_id",
    "claim_id", "case_id", "hash", "key", "surrogate",
    "segment_id", "group_id", "region"
]

# Name-based vulnerable-person indicators
NAME_BASED_VULNERABLE = [
    "age", "health", "disability", "medical", "benefit",
    "risk", "mental"
]

def infer_privacy_from_data(df):
    """
    Classifies each column into:
        - pii
        - vulnerable
        - quasi_pii
        - none
    """

    privacy_results = {}

    for col in df.columns:
        cname = col.lower()
        detected = set()

        # 1. Name-based heuristics
        if any(t in cname for t in NAME_BASED_PII):
            detected.add("pii")
        if any(t in cname for t in NAME_BASED_QUASI):
            detected.add("quasi_pii")
        if any(t in cname for t in NAME_BASED_VULNERABLE):
            detected.add("vulnerable")

        # 2. Regex-based data detection
        col_str = F.col(col).cast("string")

        for label, pattern in PII_REGEX_PATTERNS.items():
            matched = df.select(F.max(col_str.rlike(pattern))).first()[0]
            if matched:
                detected.add("pii")

        # 3. Precedence resolution
        if "pii" in detected:
            privacy_results[col] = "pii"
        elif "vulnerable" in detected:
            privacy_results[col] = "vulnerable"
        elif "quasi_pii" in detected:
            privacy_results[col] = "quasi_pii"
        else:
            privacy_results[col] = "none"

    return privacy_results

# ============================================================
#  Row Count → Volume Mapping
# ============================================================

VOLUME_ROW_RANGES = {
    "XXS": (0, 10_000),
    "XS":  (10_001, 100_000),
    "S":   (100_001, 1_000_000),
    "M":   (1_000_001, 10_000_000),
    "L":   (10_000_001, 100_000_000),
    "XL":  (100_000_001, 1_000_000_000),
    "XXL": (1_000_000_001, 10_000_000_000),
    "XXXL":(10_000_000_001, float("inf")),
}

def map_row_count_to_volume(rc):
    for size, (low, high) in VOLUME_ROW_RANGES.items():
        if low <= rc <= high:
            return size
    return "M"

# ============================================================
#  PRIMARY KEY INFERENCE
# ============================================================

PK_HINTS = ["id", "_id", "code", "_code", "pk", "uuid", "guid", "hash"]

PK_ALLOWED_TYPES = (
    StringType, IntegerType, LongType, DoubleType, FloatType,
    DecimalType, DateType, TimestampType, ShortType, ByteType, BooleanType
)

def pk_name_score(col):
    """
    Scoring heuristic based on column name strength.
    """
    c = col.lower()
    score = 0
    if c == "id": score += 5
    if c.endswith("_id"): score += 5
    if any(h in c for h in PK_HINTS): score += 3
    return score

def collect_column_stats(df, columns):
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

def infer_primary_key(df, sample_row_count, candidate_columns, stats):
    """
    Attempts to infer:
       - Single-column PK
       - Composite PKs (pairs)
    Uses sampled row counts.
    """

    if sample_row_count == 0 or not candidate_columns:
        return []

    ranked = []

    # 1 — Score columns based on name, distinct ratio, null ratio
    for col in candidate_columns:
        s = stats[col]
        dr = (s["distinct"] / sample_row_count) if sample_row_count else 0
        nr = (s["nulls"] / sample_row_count) if sample_row_count else 0
        score = pk_name_score(col)*10 + dr*50 - nr*30
        ranked.append((col, score, dr, nr))

    ranked.sort(key=lambda x: x[1], reverse=True)

    if not ranked:
        return []

    # 2 — Check best single-column PK
    best_col, best_score, best_dr, _ = ranked[0]

    if best_score > 60 and best_dr > 0.98:
        return [best_col]

    # 3 — Composite PK inference
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

# ============================================================
#  SCHEMA METADATA EXTRACTION
# ============================================================

def extract_schema_metadata(df, inferred_privacy):
    """
    Extracts column metadata using already-inferred privacy classifications.
    PK candidates collected here.
    """
    props = []
    pk_candidates = []

    for field in df.schema.fields:
        if field.name == "ingestdatetime":
            continue

        col_info = {
            "name": field.name,
            "type": field.dataType.simpleString(),
            "nullable": field.nullable,
            "description": "",
            "privacy": inferred_privacy[field.name],
            "maskingStrategy": "none",
            "isPrimaryKey": False,
            "isChangeTracking": False,
            "tags": []
        }
        props.append(col_info)

        if isinstance(field.dataType, PK_ALLOWED_TYPES):
            pk_candidates.append(field.name)

    return props, pk_candidates

# ============================================================
#  CONTRACT BUILDER (Sampling, PK, Privacy)
# ============================================================

def build_contract(domain, table, sample_fraction):
    """
    Builds a single contract for a given table using:
      - table-level sampling
      - sampled row count
      - estimated full row count
      - sampled PK inference
      - sampled privacy inference
    """

    full_name = f"{DELTA_SOURCE_CATALOG}.{DELTA_SOURCE_SCHEMA}.{table}"

    # ========================================================
    # 1 — SAMPLE THE TABLE AT READ TIME (CRITICAL)
    # ========================================================
    df = (
        spark.table(full_name)
             .sample(withReplacement=False, fraction=sample_fraction, seed=42)
             .persist(StorageLevel.MEMORY_AND_DISK)
    )

    try:
        # 2 — Row counts
        sample_row_count = df.count()
        estimated_row_count = int(sample_row_count / sample_fraction)

        # 3 — Volume classification (on estimated row count)
        volume = map_row_count_to_volume(estimated_row_count)

        # 4 — Privacy inference (works on sampled df)
        inferred_privacy = infer_privacy_from_data(df)

        # 5 — Extract schema metadata + PK candidates
        columns, pk_candidates = extract_schema_metadata(df, inferred_privacy)

        # 6 — PK inference (sample-based)
        col_stats = collect_column_stats(df, pk_candidates)
        inferred_pks = infer_primary_key(
            df,
            sample_row_count,
            pk_candidates,
            col_stats
        )

        # Mark columns that are PKs and set isChangeTracking for non-PKs
        for c in columns:
            c["isPrimaryKey"] = c["name"] in inferred_pks
            c["isChangeTracking"] = c["name"] not in inferred_pks

        # Detect whether the table contains PII
        contains_pii = any(c["privacy"] == "pii" for c in columns)

        # ========================================================
        # 7 — Build contract JSON structure
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
                {"role": "dataOwner",     "name": DEFAULT_DATA_OWNER,     "email": DEFAULT_DATA_OWNER_EMAIL},
                {"role": "seniorManager", "name": DEFAULT_SENIOR_MANAGER, "email": DEFAULT_SENIOR_MANAGER_EMAIL},
                {"role": "dataSteward",   "name": DEFAULT_DATA_STEWARD,   "email": DEFAULT_DATA_STEWARD_EMAIL},
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

                # IMPORTANT:
                #   - estimated_row_count → written to contract
                #   - sample_row_count   → NOT written to contract (only report)
                "rowCount": estimated_row_count,
                "volume": volume,
                "inferredPrimaryKeys": inferred_pks,

                "governance": {
                    "classification": DEFAULT_CLASSIFICATION,
                    "containsPersonalData": contains_pii,
                    "specialCategoryData": False,
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
                "sample_row_count": sample_row_count
            }
        }

        return contract

    finally:
        df.unpersist()

# ============================================================
#  CONTRACT WRITER
# ============================================================

def write_contract(contract):
    """
    Writes the contract YAML file unless DRY_RUN is enabled.
    """

    table = contract["schema"]["table"]

    if DRY_RUN:
        print(f"\n--- DRY RUN: {table} ---")
        print(f"Estimated row count: {contract['customProperties']['rowCount']}")
        print(f"Sample row count:    {contract['_internal']['sample_row_count']}")
        for c in contract["schema"]["properties"]:
            print(f"  {c['name']} → privacy={c['privacy']} pk={c['isPrimaryKey']}")
        print("--- END DRY RUN ---\n")
        return

    path = f"{CONTRACT_OUTPUT_PATH}/data.{contract['domain']}.{table}.yaml"

    with open(path, "w") as f:
        yaml.dump(contract, f, sort_keys=False)

    print(f"✔ Contract written: {path}")

# ============================================================
#  PARALLEL TABLE EXECUTION
# ============================================================

def analyze_tables_parallel(domain, tables, sample_fraction):
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
                print(f"✘ Contract failed for {table}: {exc}")

    # Sort results for consistent reporting
    results.sort(key=lambda x: x["table"])
    return results

# ============================================================
#  PART 4 — SAMPLE REPORT
# ============================================================

def print_sample_report(results, sample_size=SAMPLE_REPORT_SIZE):
    """
    Displays a report of the first N successfully processed tables.
    Includes sample_row_count and estimated_row_count.
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
        pii_cols = sum(1 for c in contract["schema"]["properties"] if c["privacy"] == "pii")

        total_estimated += estimated_count

        print(
            f"• {table} | "
            f"sample_rows={sample_count} | "
            f"estimated_rows={estimated_count} | "
            f"volume={volume} | "
            f"columns={len(contract['schema']['properties'])} | "
            f"pii_cols={pii_cols} | "
            f"pk={pks}"
        )

    print(f"Total estimated rows (sampled tables): {total_estimated}")
    print("=== END SAMPLE REPORT ===\n")

# ============================================================
#  TABLE ENUMERATION
# ============================================================

def get_all_tables(catalog, schema):
    """
    Returns all tables in the given UC catalog.schema.
    """
    df = spark.sql(f"SHOW TABLES IN {catalog}.{schema}")
    return [row.tableName for row in df.collect()]

def filter_tables(all_tables, table_filter):
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

# ============================================================
#  ENTRYPOINT: CONTRACTS FOR SPECIFIC TABLES
# ============================================================

def generate_contracts_for_tables(domain, tables, sample_fraction=DEFAULT_SAMPLE_FRACTION, sample_report_size=SAMPLE_REPORT_SIZE):
    """
    Generates contracts for a fixed list of tables.
    """
    tables = sorted(set(tables))

    if not tables:
        print("No tables to process.")
        return

    mode = "DRY RUN" if DRY_RUN else "WRITE MODE"
    print(f"\n=== {mode} | {len(tables)} tables | sample_fraction={sample_fraction} ===")

    results = analyze_tables_parallel(domain, tables, sample_fraction)
    print_sample_report(results, sample_report_size)

    # Write contracts
    for entry in results:
        if entry["status"] == "ok":
            write_contract(entry["contract"])
        else:
            print(f"✘ Skipping write for {entry['table']} due to errors.")

# ============================================================
#  ENTRYPOINT: CONTRACTS FOR AN ENTIRE SCHEMA
# ============================================================

def generate_contracts_for_schema(domain, table_filter=None, sample_fraction=DEFAULT_SAMPLE_FRACTION, sample_report_size=SAMPLE_REPORT_SIZE):
    """
    Generates contracts for all or filtered tables in a schema.
    """
    all_tables = get_all_tables(DELTA_SOURCE_CATALOG, DELTA_SOURCE_SCHEMA)
    tables = filter_tables(all_tables, table_filter)

    if sample_report_size==0:
        smaple_report_size=len(tables)

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
