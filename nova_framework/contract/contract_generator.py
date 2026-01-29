"""
Contract Generator for NovaFlow.

Analyzes tables and generates data contract YAML files with PII classification.
Provides configurable toggles for each privacy classification type.
"""

from datetime import datetime
from pathlib import Path
from typing import Dict, List, Optional, Any
import yaml


# ============================================================================
# PII CLASSIFICATION TOGGLE CONSTANTS
# ============================================================================
# Set to True to include detection for each classification type in generation.
# Set to False to skip detection for that classification type.

# GDPR Personal Data Categories
INCLUDE_PII = True              # Basic Personal Data (Direct identifiers) - Art. 4
INCLUDE_QUASI_PII = True        # Quasi-Personal Data (Indirect identifiers) - Art. 4
INCLUDE_SPECIAL = True          # Special Category Personal Data - Art. 9
INCLUDE_CRIMINAL = True         # Criminal Offence Data - Art. 10
INCLUDE_CHILD = True            # Children's Personal Data - Art. 8

# Industry/Risk-based Categories
INCLUDE_FINANCIAL_PII = True    # Financial Personal Data
INCLUDE_PCI = True              # Payment Card Data (PCI DSS)
INCLUDE_AUTH = True             # Authentication & Security Data
INCLUDE_LOCATION = True         # Precise Location Data
INCLUDE_TRACKING = True         # Online Identifiers / Tracking Data
INCLUDE_HR = True               # Employment / HR Data

# Non-personal but sensitive
INCLUDE_COMMERCIAL = True       # Commercially Sensitive Financial Data
INCLUDE_IP = True               # Intellectual Property / Trade Secrets


# Classification toggle map for programmatic access
CLASSIFICATION_TOGGLES = {
    "pii": INCLUDE_PII,
    "quasi_pii": INCLUDE_QUASI_PII,
    "special": INCLUDE_SPECIAL,
    "criminal": INCLUDE_CRIMINAL,
    "child": INCLUDE_CHILD,
    "financial_pii": INCLUDE_FINANCIAL_PII,
    "pci": INCLUDE_PCI,
    "auth": INCLUDE_AUTH,
    "location": INCLUDE_LOCATION,
    "tracking": INCLUDE_TRACKING,
    "hr": INCLUDE_HR,
    "commercial": INCLUDE_COMMERCIAL,
    "ip": INCLUDE_IP,
}


# Default masking strategies per classification
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
    "commercial": "none",
    "ip": "none",
}


class ContractGenerator:
    """
    Generates data contract YAML files from table analysis.

    Analyzes table schemas and sample data to detect PII classifications,
    then generates compliant data contract YAML files.
    """

    # Column name patterns for PII detection
    PII_PATTERNS = {
        "pii": [
            "name", "first_name", "last_name", "full_name", "email", "phone",
            "mobile", "telephone", "address", "street", "city", "postcode",
            "zip_code", "ssn", "national_insurance", "ni_number", "passport",
            "driver_license", "date_of_birth", "dob", "birth_date"
        ],
        "quasi_pii": [
            "age", "gender", "ethnicity", "nationality", "country", "region",
            "county", "state", "occupation", "job_title", "department",
            "salary_band", "age_group", "income_bracket"
        ],
        "special": [
            "race", "ethnic_origin", "religion", "religious_belief", "political",
            "trade_union", "health", "medical", "genetic", "biometric", "sexual",
            "disability", "mental_health"
        ],
        "criminal": [
            "criminal", "conviction", "offense", "offence", "sentence", "arrest",
            "charge", "court", "probation", "parole"
        ],
        "child": [
            "child_name", "minor", "guardian", "parent_consent", "child_age",
            "school", "pupil"
        ],
        "financial_pii": [
            "salary", "wage", "income", "bonus", "pension", "tax", "ni_contrib",
            "bank_account", "iban", "sort_code", "account_number"
        ],
        "pci": [
            "card_number", "credit_card", "debit_card", "cvv", "cvc", "expiry",
            "pan", "card_holder"
        ],
        "auth": [
            "password", "pin", "secret", "token", "api_key", "private_key",
            "credential", "auth_code", "mfa", "otp"
        ],
        "location": [
            "latitude", "longitude", "lat", "lng", "gps", "coordinates",
            "geolocation", "geo_point"
        ],
        "tracking": [
            "ip_address", "mac_address", "device_id", "cookie", "session_id",
            "user_agent", "fingerprint", "tracking_id"
        ],
        "hr": [
            "employee_id", "staff_number", "manager", "hire_date", "termination",
            "performance", "review", "disciplinary", "grievance", "absence"
        ],
        "commercial": [
            "revenue", "profit", "margin", "cost", "price", "budget", "forecast",
            "target", "commission"
        ],
        "ip": [
            "patent", "trademark", "copyright", "trade_secret", "proprietary",
            "confidential_formula", "algorithm", "source_code"
        ],
    }

    def __init__(
        self,
        spark,
        env: str,
        output_volume_path: Optional[str] = None
    ):
        """
        Initialize the contract generator.

        Args:
            spark: SparkSession instance
            env: Environment name (dev, uat, prod)
            output_volume_path: Path to write contracts (defaults to volume)
        """
        self.spark = spark
        self.env = env
        self.output_path = output_volume_path or (
            f"/Volumes/cluk_{env}_nova/nova_framework/data_contracts"
        )

    def is_classification_enabled(self, classification: str) -> bool:
        """Check if a PII classification type is enabled for detection."""
        return CLASSIFICATION_TOGGLES.get(classification, False)

    def detect_column_classification(self, column_name: str) -> str:
        """
        Detect PII classification for a column based on name patterns.

        Only returns classifications that are enabled via toggle constants.

        Args:
            column_name: Name of the column to classify

        Returns:
            Classification string or "none"
        """
        col_lower = column_name.lower()

        for classification, patterns in self.PII_PATTERNS.items():
            if not self.is_classification_enabled(classification):
                continue

            for pattern in patterns:
                if pattern in col_lower:
                    return classification

        return "none"

    def generate_contract(
        self,
        table_name: str,
        schema_name: str,
        domain: str,
        sample_rows: int = 1000,
        data_owner: Optional[Dict[str, str]] = None,
        data_steward: Optional[Dict[str, str]] = None
    ) -> Dict[str, Any]:
        """
        Analyze a table and generate a data contract.

        Args:
            table_name: Name of the table to analyze
            schema_name: Schema/catalog containing the table
            domain: Business domain for the contract
            sample_rows: Number of rows to sample for analysis
            data_owner: Data owner contact info
            data_steward: Data steward contact info

        Returns:
            Dictionary containing the generated contract
        """
        full_table_name = f"{schema_name}.{table_name}"
        start_time = datetime.now()

        print(f"[ContractGenerator] {full_table_name} - Analysis started at {start_time.strftime('%H:%M:%S')}")

        # Read table schema and sample
        df = self.spark.table(full_table_name)
        total_rows = df.count()
        sampled_rows = min(sample_rows, total_rows)

        # Get schema information
        schema_fields = df.schema.fields

        # Build column properties with PII detection
        properties = []
        for field in schema_fields:
            classification = self.detect_column_classification(field.name)
            masking_strategy = DEFAULT_MASKING_STRATEGIES.get(classification, "none")

            col_props = {
                "name": field.name,
                "type": self._spark_type_to_string(field.dataType),
                "nullable": field.nullable,
                "privacy": classification,
                "maskingStrategy": masking_strategy,
            }
            properties.append(col_props)

        end_time = datetime.now()
        duration = (end_time - start_time).total_seconds()

        print(f"[ContractGenerator] {full_table_name} - Analysis completed at {end_time.strftime('%H:%M:%S')} ({duration:.2f}s), {sampled_rows} rows sampled")

        # Build contract structure
        contract = self._build_contract_structure(
            table_name=table_name,
            schema_name=schema_name,
            domain=domain,
            properties=properties,
            data_owner=data_owner,
            data_steward=data_steward
        )

        return contract

    def _spark_type_to_string(self, spark_type) -> str:
        """Convert Spark data type to contract type string."""
        type_name = spark_type.typeName()
        type_map = {
            "string": "string",
            "integer": "integer",
            "long": "integer",
            "double": "double",
            "float": "double",
            "decimal": "double",
            "date": "date",
            "timestamp": "timestamp",
            "boolean": "boolean",
        }
        return type_map.get(type_name, "string")

    def _build_contract_structure(
        self,
        table_name: str,
        schema_name: str,
        domain: str,
        properties: List[Dict],
        data_owner: Optional[Dict[str, str]] = None,
        data_steward: Optional[Dict[str, str]] = None
    ) -> Dict[str, Any]:
        """Build the full contract YAML structure."""
        contract_name = f"data.{domain}.{table_name}"

        # Determine if contract contains personal data
        contains_personal_data = any(
            p.get("privacy", "none") != "none" for p in properties
        )

        # Determine highest classification
        classification = "Public"
        for prop in properties:
            privacy = prop.get("privacy", "none")
            if privacy in ("special", "criminal", "child", "pci", "auth"):
                classification = "Restricted"
                break
            elif privacy in ("pii", "quasi_pii", "financial_pii", "hr"):
                classification = "Confidential"

        contract = {
            "apiVersion": "v3.0.2",
            "kind": "DataContract",
            "name": contract_name,
            "version": "1.0.0",
            "domain": domain,
            "dataProduct": table_name,
            "team": [],
            "schema": {
                "name": schema_name,
                "table": table_name,
                "format": "delta",
                "description": f"Auto-generated contract for {table_name}",
                "properties": properties,
            },
            "customProperties": {
                "governance": {
                    "classification": classification,
                    "containsPersonalData": contains_personal_data,
                    "specialCategoryData": any(
                        p.get("privacy") == "special" for p in properties
                    ),
                    "retentionPeriod": "7 years",
                    "uk_gdpr_lawful_basis": "Legitimate Interest",
                },
                "accessControl": {
                    "read": [f"{domain}_readers"],
                    "readSensitive": [f"{domain}_sensitive_readers"],
                    "write": [f"{domain}_writers"],
                },
                "volume": "M",
            },
        }

        # Add team members if provided
        if data_owner:
            contract["team"].append({
                "role": "dataOwner",
                **data_owner
            })
        if data_steward:
            contract["team"].append({
                "role": "dataSteward",
                **data_steward
            })

        return contract

    def write_contract(
        self,
        contract: Dict[str, Any],
        filename: Optional[str] = None
    ) -> str:
        """
        Write contract to YAML file in the output volume.

        Args:
            contract: Contract dictionary to write
            filename: Optional filename (defaults to contract name)

        Returns:
            Path to written file
        """
        if filename is None:
            filename = f"{contract['name']}.yaml"

        output_file = Path(self.output_path) / filename

        # Ensure directory exists
        output_file.parent.mkdir(parents=True, exist_ok=True)

        # Write YAML
        with open(output_file, "w") as f:
            yaml.dump(contract, f, default_flow_style=False, sort_keys=False)

        print(f"[ContractGenerator] Contract written to: {output_file}")

        return str(output_file)

    def generate_and_write(
        self,
        table_name: str,
        schema_name: str,
        domain: str,
        sample_rows: int = 1000,
        data_owner: Optional[Dict[str, str]] = None,
        data_steward: Optional[Dict[str, str]] = None
    ) -> str:
        """
        Generate a contract and write it to file in one operation.

        Args:
            table_name: Name of the table to analyze
            schema_name: Schema/catalog containing the table
            domain: Business domain for the contract
            sample_rows: Number of rows to sample for analysis
            data_owner: Data owner contact info
            data_steward: Data steward contact info

        Returns:
            Path to written contract file
        """
        contract = self.generate_contract(
            table_name=table_name,
            schema_name=schema_name,
            domain=domain,
            sample_rows=sample_rows,
            data_owner=data_owner,
            data_steward=data_steward
        )

        return self.write_contract(contract)


def get_enabled_classifications() -> List[str]:
    """Return list of currently enabled PII classifications."""
    return [k for k, v in CLASSIFICATION_TOGGLES.items() if v]


def get_disabled_classifications() -> List[str]:
    """Return list of currently disabled PII classifications."""
    return [k for k, v in CLASSIFICATION_TOGGLES.items() if not v]
