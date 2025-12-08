from pyspark.sql.types import (
    StructType, StructField, StringType, IntegerType,
    DoubleType, DateType, TimestampType, DecimalType
)
import yaml
from pathlib import Path
from typing import List


class DataContract:
    """
    Loader and accessor for Nova + ODCS v3.0.2 aligned data contracts.
    Provides:
      - Access to contract fields via Python properties
      - Derived attributes such as pipeline_type, natural keys,
        change-tracking columns, recommended partitions.
      - Mapping from ODCS schema.properties → Spark StructType
    """

    # --------------------------
    # STATIC CONFIGURATIONS
    # --------------------------

    VOLUME_PARTITION_MAP = {
        "XXS": 1,
        "XS": 10,
        "S": 25,
        "M": 50,
        "L": 100,
        "XL": 200,
        "XXL": 500,
        "XXXL": 1000
    }

    BASE_PATH_TEMPLATE = (
        "/Volumes/cluk_{env}_nova/nova_framework/data_contracts"
    )

    TYPE_MAP = {
        "string": StringType(),
        "int": IntegerType(),
        "integer": IntegerType(),
        "double": DoubleType(),
        "date": DateType(),
        "timestamp": TimestampType(),
    }


    # ======================================================================
    # QUALITY RULES SEVERITY → WEIGHT MAP
    # ======================================================================
    SEVERITY_WEIGHTS = {
        "critical": 5,
        "warning": 3,
        "info": 1
    }

    # --------------------------
    # INITIALISATION
    # --------------------------

    def __init__(self, contract_name: str, env: str):
        self.env = env
        self.contract_name = contract_name

        self.path = Path(
            self.BASE_PATH_TEMPLATE.format(env=env)
        ) / f"{contract_name}.yaml"

        self._contract = self._load_contract()
        self._spark_schema_cache = None

    def _load_contract(self):
        if not self.path.exists():
            raise FileNotFoundError(f"Contract not found: {self.path}")

        with self.path.open("r") as f:
            return yaml.safe_load(f) or {}

    # --------------------------------------------------------------------
    # GENERIC GETTER (dot notation)
    # --------------------------------------------------------------------

    def get(self, key: str, default=None):
        """
        Get nested values using dot notation.
        Example: get("schema.properties.0.name")
        """
        value = self._contract
        for part in key.split("."):
            if isinstance(value, dict) and part in value:
                value = value[part]
            elif isinstance(value, list):
                try:
                    idx = int(part)
                    value = value[idx]
                except (ValueError, IndexError):
                    return default
            else:
                return default
        return value

    # --------------------------------------------------------------------
    # CORE CONTRACT METADATA
    # --------------------------------------------------------------------

    @property
    def api_version(self): return self._contract.get("apiVersion")

    @property
    def kind(self): return self._contract.get("kind")

    @property
    def name(self): return self._contract.get("name")

    @property
    def version(self): return self._contract.get("version")

    @property
    def domain_name(self): return self._contract.get("domain")

    @property
    def data_product(self): return self._contract.get("dataProduct")

    # --------------------------------------------------------------------
    # TEAM
    # --------------------------------------------------------------------

    @property
    def team(self): return self._contract.get("team", [])

    def get_team_role(self, role: str):
        for member in self.team:
            if member.get("role") == role:
                return member
        return None

    @property
    def data_owner(self): return self.get_team_role("dataOwner")

    @property
    def senior_manager(self): return self.get_team_role("seniorManager")

    @property
    def data_steward(self): return self.get_team_role("dataSteward")

    # --------------------------------------------------------------------
    # SCHEMA FIELDS
    # --------------------------------------------------------------------

    @property
    def schema(self): return self._contract.get("schema", {})

    @property
    def schema_name(self): return self.schema.get("name").lower()

    @property
    def table_name(self): return self.schema.get("table").lower()

    @property
    def schema_format(self): return self.schema.get("format").lower()

    @property
    def schema_description(self): return self.schema.get("description")

    @property
    def schema_tags(self): return self.schema.get("tags", [])

    @property
    def columns(self): return self.schema.get("properties", [])

    # --------------------------------------------------------------------
    # SPARK SCHEMA MAPPING
    # --------------------------------------------------------------------

    @property
    def spark_schema(self) -> StructType:
        if self._spark_schema_cache is None:
            fields = []

            for col in self.columns:
                name = col["name"]
                type_str = col.get("type", "string").lower()
                nullable = col.get("nullable", True)

                # Parse decimal types
                if type_str.startswith("decimal"):
                    spark_type = self._parse_decimal_type(type_str)
                else:
                    spark_type = self.TYPE_MAP.get(type_str, StringType())

                fields.append(StructField(name, spark_type, nullable))

            self._spark_schema_cache = StructType(fields)

        return self._spark_schema_cache

    def _parse_decimal_type(self, type_str: str) -> DecimalType:
        """
        Parse decimal type string to DecimalType.

        Supports:
        - "decimal" -> DecimalType(10, 0)
        - "decimal(38,0)" -> DecimalType(38, 0)
        - "decimal(38.0)" -> DecimalType(38, 0)
        - "decimal(10,2)" -> DecimalType(10, 2)

        Args:
            type_str: Type string from contract (e.g., "decimal(38,0)" or "decimal(38.0)")

        Returns:
            DecimalType with specified precision and scale
        """
        import re

        # Default decimal type
        if type_str == "decimal":
            return DecimalType(10, 0)

        # Parse decimal(precision, scale) - handle both comma and period separators
        match = re.match(r"decimal\((\d+)[,.\s]+(\d+)\)", type_str)
        if match:
            precision = int(match.group(1))
            scale = int(match.group(2))
            return DecimalType(precision, scale)

        # Fallback to default
        return DecimalType(10, 0)

    # --------------------------------------------------------------------
    # DERIVED: PIPELINE TYPE
    # --------------------------------------------------------------------

    @property
    def pipeline_type(self) -> str:
        schema_name = (self.schema.get("name") or "").lower()

        if schema_name.startswith("bronze_"):
            return "ingestion"
        if schema_name.startswith("silver_"):
            return "transformation"
        if schema_name.startswith("gold_"):
            return "transformation"

        return "unknown"

    # --------------------------------------------------------------------
    # DERIVED: NATURAL KEYS & CHANGE TRACKING
    # --------------------------------------------------------------------

    @property
    def natural_key_columns(self) -> List[str]:
        return [c["name"] for c in self.columns if c.get("isPrimaryKey")]

    @property
    def change_tracking_columns(self) -> List[str]:
        return [c["name"] for c in self.columns if c.get("isChangeTracking")]

    # --------------------------------------------------------------------
    # CUSTOM PROPERTIES
    # --------------------------------------------------------------------

    @property
    def custom(self): return self._contract.get("customProperties", {})

    @property
    def governance(self): return self.custom.get("governance", {})

    @property
    def access_control(self): return self.custom.get("accessControl", {})


    @property
    def source_format(self): return "parquet"

    # Volume + partition logic
    @property
    def volume(self): return self.custom.get("volume", "M")

    @property
    def recommended_partitions(self) -> int:
        return self.VOLUME_PARTITION_MAP.get(self.volume, 50)
   
    @property
    def csv_options(self) -> dict:
        """
        Returns a standardised dictionary of Spark CSV read options.
        If the contract defines customProperties.csvOptions, these override defaults.
        """
        default_options = {
            "header": "true",
            "inferSchema": "false",
            "delimiter": ",",
            "mode": "PERMISSIVE",
            "quote": '"',
            "escape": '"',
            "multiLine": "false",
            "ignoreLeadingWhiteSpace": "true",
            "ignoreTrailingWhiteSpace": "true"
        }

        contract_options = self.custom.get("csvOptions", {})

        # Merge with defaults (contract overrides default)
        return {**default_options, **contract_options}

    @property
    def source_file_csv_options(self) -> dict:
        """
        Returns a standardised dictionary of Spark CSV read options.
        If the contract defines customProperties.csvOptions, these override defaults.
        """
        default_options = {
            "header": "true",
            "inferSchema": "false",
            "delimiter": ",",
            "mode": "PERMISSIVE",
            "quote": '"',
            "escape": '"',
            "multiLine": "false",
            "ignoreLeadingWhiteSpace": "true",
            "ignoreTrailingWhiteSpace": "true"
        }

        contract_options = self.custom.get("csvOptions", {})

        # Merge with defaults (contract overrides default)
        return {**default_options, **contract_options}

    @property
    def source_file_json_options(self) -> dict:
        """
        Returns a standardised dictionary of Spark JSON read options.
        If the contract defines customProperties.jsonOptions, these override defaults.
        """
        default_options = {
            "mode": "PERMISSIVE",
            "multiLine": "false"
        }

        contract_options = self.custom.get("jsonOptions", {})

        # Merge with defaults (contract overrides default)
        return {**default_options, **contract_options}

    # ======================================================================
    # EXTRACT RAW RULES (from YAML)
    # ======================================================================
    @property
    def raw_quality_rules(self):
        return self._contract.get("quality", [])

    # ======================================================================
    # EXPAND RULES HELPER FUNCTION
    # ======================================================================
    def _expand_columns(self, explicit_cols, allowed_types=None):
        """
        Expand the user-specified `columns:` list.

        Supports:
        - ["*"] → all columns (optionally filtered by type)
        - ["A"] → ["A"]
        - ["A","B"] → ["A","B"]
        - None → []

        Parameters:
        - explicit_cols: list or None from the contract rule
        - allowed_types: optional list of Spark/Nova types to filter on
        """
        if explicit_cols is None:
            return []

        # Wildcard ("*")
        if explicit_cols == ["*"]:
            if allowed_types:
                return [
                    c["name"]
                    for c in self.columns
                    if c.get("type") in allowed_types
                ]
            return [c["name"] for c in self.columns]

        # Explicit list
        return explicit_cols
   
    # ======================================================================
    # GENERIC CLEANSING RULE EXTRACTION
    # ======================================================================
    @property
    def cleansing_rules(self):
        rules = []

        for rule in self.raw_quality_rules:
            if rule.get("type") != "transformation":
                continue

            name = rule["rule"]
            explicit_cols = rule.get("columns")

            # ------------------------------------------------------------------
            # trim_string_fields
            # ------------------------------------------------------------------
            if name == "trim_string_fields":
                target_cols = (
                    self._expand_columns(explicit_cols, allowed_types=["string"])
                    if explicit_cols
                    else [c["name"] for c in self.columns if c["type"] == "string"]
                )
                for col in target_cols:
                    rules.append({"rule": "trim", "column": col})

            # ------------------------------------------------------------------
            # uppercase
            # ------------------------------------------------------------------
            elif name == "uppercase":
                target_cols = self._expand_columns(explicit_cols, allowed_types=["string"])
                for col in target_cols:
                    rules.append({"rule": "upper", "column": col})

            # ------------------------------------------------------------------
            # replace_empty_strings_with_null
            # ------------------------------------------------------------------
            elif name == "replace_empty_strings_with_null":
                target_cols = (
                    self._expand_columns(explicit_cols, allowed_types=["string"])
                    if explicit_cols
                    else [c["name"] for c in self.columns if c["type"] == "string"]
                )
                for col in target_cols:
                    rules.append({
                        "rule": "regex_replace",
                        "column": col,
                        "pattern": "^$",
                        "replacement": None
                    })

            # ------------------------------------------------------------------
            # nullify_empty_strings
            # ------------------------------------------------------------------
            elif name == "nullify_empty_strings":
                target_cols = self._expand_columns(explicit_cols, allowed_types=["string"])
                for col in target_cols:
                    rules.append({
                        "rule": "nullify_empty_strings",
                        "column": col
                    })

            # ------------------------------------------------------------------
            # collapse_whitespace
            # ------------------------------------------------------------------
            elif name == "collapse_whitespace":
                target_cols = self._expand_columns(explicit_cols, allowed_types=["string"])
                for col in target_cols:
                    rules.append({
                        "rule": "regex_replace",
                        "column": col,
                        "pattern": r"\s+",
                        "replacement": " "
                    })

            # ------------------------------------------------------------------
            # normalize_boolean_values
            # ------------------------------------------------------------------
            elif name == "normalize_boolean_values":
                target_cols = self._expand_columns(explicit_cols)
                for col in target_cols:
                    rules.append({
                        "rule": "normalize_boolean_values",
                        "column": col
                    })

            # ------------------------------------------------------------------
            # remove_control_characters
            # ------------------------------------------------------------------
            elif name == "remove_control_characters":
                target_cols = self._expand_columns(explicit_cols, allowed_types=["string"])
                for col in target_cols:
                    rules.append({
                        "rule": "regex_replace",
                        "column": col,
                        "pattern": r"[\\x00-\\x1F\\x7F]",
                        "replacement": ""
                    })

        return rules


    # ======================================================================
    # GENERIC DATA QUALITY RULE EXTRACTION
    # ======================================================================
    @property
    def quality_rules(self):
        """
        Expand declarative DQ rules from the data contract into the
        fully materialized rule list expected by the DQEngine.

        - Supports: not_null, not_blank, allowed_values, regex,
                    min_length, max_length, digits_only, letters_only,
                    min, max, between, is_number, is_date, unique

        - Applies severity weights.
        - Expands multi-column definitions (columns: [])
        - Skips transformation rules.
        """

        dq_rules = []

        # Severity → Weight
        severity_weight = {
            "critical": 5,
            "warning": 3,
            "info": 1
        }

        for rule in self.raw_quality_rules:

            # Skip cleansing rules — only process evaluation rules
            if rule.get("type") == "transformation":
                continue

            name = rule["rule"]
            severity = rule.get("severity", "info")
            weight = severity_weight.get(severity, 1)

            # Column handling: allow `column:` or `columns:`
            if "columns" in rule:
                target_cols = rule["columns"]
            elif "column" in rule:
                target_cols = [rule["column"]]
            else:
                continue  # no column specified → cannot apply

            # ------------------------------------------------------------------
            # Expand rule for each target column
            # ------------------------------------------------------------------
            for col in target_cols:

                # Already-supported built-in rules
                if name == "not_null":
                    dq_rules.append({
                        "rule": "not_null",
                        "column": col,
                        "weight": weight
                    })

                elif name == "regex":
                    dq_rules.append({
                        "rule": "regex",
                        "column": col,
                        "pattern": rule["pattern"],
                        "weight": weight
                    })

                elif name == "allowed_values":
                    dq_rules.append({
                        "rule": "allowed_values",
                        "column": col,
                        "values": rule["values"],
                        "weight": weight
                    })

                elif name == "min":
                    dq_rules.append({
                        "rule": "min",
                        "column": col,
                        "value": rule["value"],
                        "weight": weight
                    })

                elif name == "max":
                    dq_rules.append({
                        "rule": "max",
                        "column": col,
                        "value": rule["value"],
                        "weight": weight
                    })

                elif name == "unique":
                    dq_rules.append({
                        "rule": "unique",
                        "column": col,
                        "weight": weight
                    })

                # ------------------------------------------------------------------
                # NEW DQ RULES (Option C)
                # ------------------------------------------------------------------

                elif name == "not_blank":
                    dq_rules.append({
                        "rule": "not_blank",
                        "column": col,
                        "weight": weight
                    })

                elif name == "min_length":
                    dq_rules.append({
                        "rule": "min_length",
                        "column": col,
                        "value": rule["value"],
                        "weight": weight
                    })

                elif name == "max_length":
                    dq_rules.append({
                        "rule": "max_length",
                        "column": col,
                        "value": rule["value"],
                        "weight": weight
                    })

                elif name == "digits_only":
                    dq_rules.append({
                        "rule": "digits_only",
                        "column": col,
                        "weight": weight
                    })

                elif name == "letters_only":
                    dq_rules.append({
                        "rule": "letters_only",
                        "column": col,
                        "weight": weight
                    })

                elif name == "between":
                    dq_rules.append({
                        "rule": "between",
                        "column": col,
                        "min": rule["min"],
                        "max": rule["max"],
                        "weight": weight
                    })

                elif name == "is_number":
                    dq_rules.append({
                        "rule": "is_number",
                        "column": col,
                        "weight": weight
                    })

                elif name == "is_date":
                    dq_rules.append({
                        "rule": "is_date",
                        "column": col,
                        "format": rule.get("format", "yyyy-MM-dd"),
                        "weight": weight
                    })

                # ------------------------------------------------------------------
                # Unknown rule → skip (or raise)
                # ------------------------------------------------------------------
                else:
                    # Optionally log / raise for unknown rule types.
                    continue

        return dq_rules

# ==== DQ EXTENSIONS ====


    # =======================================================
    # Column Expansion Helper
    # =======================================================
    def _expand_columns(self, explicit_cols, allowed_types=None):
        if explicit_cols is None:
            return []
        # wildcard
        if explicit_cols == ["*"]:
            if allowed_types:
                return [c["name"] for c in self.columns if c.get("type") in allowed_types]
            return [c["name"] for c in self.columns]
        return explicit_cols

    # =======================================================
    # Validation (Option B: warn + skip)
    # =======================================================
    def _validate_columns(self, cols, rule_name):
        import logging
        valid = []
        for c in cols:
            if c not in {col["name"] for col in self.columns}:
                logging.warning(f"[DataContract] Rule '{rule_name}' references missing column '{c}'. Skipping.")
            else:
                valid.append(c)
        return valid

    

