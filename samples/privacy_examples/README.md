# Sensitivity-Aware Privacy Examples

This directory contains example configuration files demonstrating the enhanced **sensitivity-aware** privacy and masking system.

## Overview

The privacy system now supports **explicit sensitivity-level access control** through the `sensitive_access` field in domain roles. This provides granular control over which AD groups can see which sensitivity levels of data unmasked.

## Key Concepts

### 1. Sensitivity Classifications (GDPR-aligned)

Defined in data contracts on each column via the `privacy` field:

| Classification | GDPR Category | Examples | Default Masking |
|----------------|---------------|----------|-----------------|
| **`pii`** | Art. 4 - Basic Personal Data | first_name, email, phone | Hash |
| **`quasi_pii`** | Art. 4 - Quasi-identifiers | postcode, age, gender | Partial |
| **`special`** | Art. 9 - Special Category | religion, health, ethnicity | Redact |
| **`criminal`** | Art. 10 - Criminal Data | criminal_record | Redact |
| **`child`** | Art. 8 - Children's Data | child_name, child_dob | Redact |
| **`financial_pii`** | Financial Personal Data | bank_account, salary | Hash/Token |
| **`pci`** | PCI DSS - Payment Card | pan, cvv | Redact |
| **`auth`** | Authentication Data | password_hash, api_key | Redact |
| **`location`** | Precise Location | gps_latitude, home_address | Partial |
| **`tracking`** | Online Identifiers | ip_address, cookie_id | Hash |
| **`hr`** | Employment/HR Data | performance_rating | Hash |
| **`commercial`** | Commercial Financial | revenue, margin | None |
| **`ip`** | Intellectual Property | algorithm_details | None |

### 2. Domain Roles with `sensitive_access`

Domain roles now explicitly declare which sensitivity levels they can access unmasked:

```yaml
roles:
  analyst.general:
    inherits: analyst
    scope:
      exclude: [sensitive_table]
    sensitive_access: [quasi_pii]  # Can only see quasi-identifiers
    description: "General analysts"

  analyst.sensitive:
    inherits: analyst
    scope:
      exclude: [sensitive_table]
    sensitive_access: [pii, quasi_pii]  # Can see PII + quasi-identifiers
    description: "Analysts with PII access"
```

### 3. Masking Logic

For each column:

1. **Check column's sensitivity** (from data contract `privacy` field)
2. **Check user's role** (from AD group → domain role mapping)
3. **Check role's `sensitive_access` list**
4. **Apply masking rule:**
   - If sensitivity **IN** `sensitive_access` → **UNMASKED** (clear text)
   - If sensitivity **NOT IN** `sensitive_access` → **MASKED** (hash/redact/etc)

## Example Files

### [`domain.roles.example.yaml`](domain.roles.example.yaml)

Complete example of domain roles with various `sensitive_access` configurations:
- `analyst.general` - Only quasi-identifiers
- `analyst.sensitive` - PII + quasi-identifiers
- `finance-specialist.customer-services` - PII + financial data
- `hr-specialist` - PII + HR + special category
- `compliance-officer` - Full access to all sensitivity levels

### [`domain.mappings.example.yaml`](domain.mappings.example.yaml)

Maps domain roles to AD groups and shows expected masking behavior for each group.

### [`data.finance.employee_salary.example.yaml`](data.finance.employee_salary.example.yaml)

Example data contract with multiple sensitivity levels:
- **3 columns** with `quasi_pii` (age_band, job_level, gender)
- **5 columns** with `pii` (first_name, email, phone, etc.)
- **5 columns** with `financial_pii` (salary, bank_account, tax_code)
- **2 columns** with `hr` (performance_rating, disciplinary_flag)
- **3 columns** with `special` (disability_flag, ethnicity, religion)

## Usage

### Setting Up Roles

1. **Define global roles** (if not exists):
   ```yaml
   # /Volumes/{catalog}/contract_registry/access/global_roles.yaml
   roles:
     analyst:
       privileges: [SELECT]
     finance-specialist:
       privileges: [SELECT]
   ```

2. **Define domain roles with `sensitive_access`**:
   ```yaml
   # /Volumes/{catalog}/contract_registry/access/domains/{domain}/domain.roles.yaml
   domain: bronze_finance

   roles:
     finance-specialist.customer-services:
       inherits: finance-specialist
       scope:
         include: [employee_salary, employee_compensation]
       sensitive_access: [pii, financial_pii]  # ⭐ NEW
       description: "Customer services - salary data access"
   ```

3. **Map roles to AD groups**:
   ```yaml
   # /Volumes/{catalog}/contract_registry/access/domains/{domain}/domain.mappings.dev.yaml
   domain: bronze_finance

   mappings:
     finance-specialist.customer-services:
       ad_groups:
         - CLUK-CAZ-EDP-dev-customer-services-finance-advisors
   ```

### Defining Sensitivity in Contracts

Add `privacy` classification to each column:

```yaml
schema:
  properties:
    - name: EMAIL
      type: string
      privacy: pii  # ⭐ Sensitivity classification
      maskingStrategy: mask_email  # Optional override

    - name: SALARY
      type: decimal(18,2)
      privacy: financial_pii  # ⭐ Financial PII
      maskingStrategy: hash  # Optional override
```

### Applying Security

```python
from pyspark.sql import SparkSession
from nova_framework.contract.contract import DataContract
from nova_framework.access import StandaloneAccessControlTool

spark = SparkSession.builder.getOrCreate()

# Load contract
contract = DataContract(
    contract_name="data.finance.employee_salary",
    env="dev"
)

# Apply full security (access control + privacy)
tool = StandaloneAccessControlTool(spark, environment="dev")

access_result, privacy_result = tool.apply_full_security(
    contract=contract,
    catalog="cluk_dev_nova"
)

print(f"Access Control: {access_result.grants_succeeded} grants")
print(f"Privacy: {privacy_result.policies_created} masking policies")
```

## Generated SQL Example

For a column with `privacy: pii`, the system generates:

```sql
ALTER TABLE bronze_finance.employee_salary
ALTER COLUMN EMAIL
SET MASK CASE
    -- Groups with 'pii' in their sensitive_access
    WHEN is_account_group_member('CLUK-CAZ-EDP-dev-data-analysts-sensitive') THEN EMAIL
    WHEN is_account_group_member('CLUK-CAZ-EDP-dev-customer-services-finance-advisors') THEN EMAIL
    WHEN is_account_group_member('CLUK-CAZ-EDP-dev-hr-team') THEN EMAIL
    -- All other groups see masked data
    ELSE CASE
        WHEN EMAIL IS NOT NULL AND INSTR(EMAIL, '@') > 0 THEN
            CONCAT(SUBSTRING(EMAIL, 1, 2), '***@', SUBSTRING(EMAIL, INSTR(EMAIL, '@') + 1))
        ELSE NULL
    END
END
```

## Benefits

1. **Granular Control**: Different roles see different sensitivity levels
2. **GDPR Compliance**: Aligned with GDPR categories (Art. 4, 8, 9, 10)
3. **Least Privilege**: `sensitive_access: [none]` = all data masked
4. **Flexible**: Mix sensitivity levels per role (e.g., `[pii, hr]`)
5. **Auditable**: Clear declaration of who can see what
6. **Scalable**: Applies automatically to all tables in domain

## Migration from Old System

**Old System** (automatic):
- Groups with table access → See ALL sensitive data unmasked
- Groups without table access → See ALL sensitive data masked

**New System** (explicit):
- Groups with table access + appropriate `sensitive_access` → See SOME sensitive data unmasked
- Other groups → See that sensitivity level masked

**Backward Compatibility:**
- If `sensitive_access` field is missing, defaults to empty `[]` (all data masked)
- To restore old behavior, use `sensitive_access: [pii, quasi_pii, special, ...]` (all levels)

## Testing

Preview changes before applying:

```python
preview = tool.preview_privacy_changes(
    contract=contract,
    catalog="cluk_dev_nova"
)

for create in preview['creates']:
    print(f"Column: {create['column']}")
    print(f"  Strategy: {create['strategy']}")
    print(f"  Exempt groups: {create['exempt_groups']}")
```

## References

- [GDPR Article 4](https://gdpr-info.eu/art-4-gdpr/) - Personal Data definitions
- [GDPR Article 9](https://gdpr-info.eu/art-9-gdpr/) - Special Category Data
- [GDPR Article 10](https://gdpr-info.eu/art-10-gdpr/) - Criminal Data
- [PCI DSS](https://www.pcisecuritystandards.org/) - Payment Card Industry standards
