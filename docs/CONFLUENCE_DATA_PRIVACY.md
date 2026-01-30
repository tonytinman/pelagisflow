# NovaFlow Data Privacy Control

---

**Page Owner**: Data Governance Team
**Last Updated**: January 2026
**Version**: 1.0
**Related Pages**:
- [NovaFlow Overview](#)
- [Data Contracts Guide](#)
- [Role-Based Access Control](#)
- [GDPR Compliance](#)

---

## 📋 Table of Contents

1. [Overview](#overview)
2. [How Privacy Works in NovaFlow](#how-privacy-works)
3. [Defining Privacy in Data Contracts](#defining-privacy-in-contracts)
4. [Defining Sensitivity Access in Roles](#defining-sensitivity-access)
5. [What Happens Automatically](#what-happens-automatically)
6. [Examples](#examples)
7. [FAQs](#faqs)
8. [Getting Help](#getting-help)

---

## Overview {#overview}

NovaFlow provides **automated, policy-driven data privacy controls** that protect sensitive data at the column level while allowing authorized users to access data they need for their roles.

### Key Principles

✅ **Privacy by Configuration**: Define once in contracts and roles, enforce everywhere automatically
✅ **GDPR Aligned**: 13 sensitivity categories aligned with GDPR Articles 4, 8, 9, and 10
✅ **Role-Based Access**: Different teams see different levels of masking based on their role
✅ **Zero-Touch Enforcement**: Framework applies masking automatically, no manual steps
✅ **Compliance by Design**: Audit trails and differential enforcement built-in

### How It Works (30-Second Version)

1. **Data Steward** marks columns as `pii`, `special`, `financial_pii`, etc. in the data contract
2. **Domain Owner** defines which roles can see which sensitivity levels in `domain.roles.yaml`
3. **NovaFlow** automatically applies Unity Catalog column masking with role-based exemptions
4. **Users** query tables normally—they see unmasked or masked data based on their AD group

> 💡 **Key Insight**: Users never see "access denied" errors. They always get data back, but sensitive columns are automatically masked if they don't have the appropriate role.

---

## How Privacy Works in NovaFlow {#how-privacy-works}

### The Two-Layer Security Model

NovaFlow implements a **two-layer security model**:

| Layer | What It Controls | How It's Defined | Enforced By |
|-------|------------------|------------------|-------------|
| **Layer 1: Table-Level Access** | Who can see the table at all | `domain.roles.yaml` (scope) | Unity Catalog GRANT/REVOKE |
| **Layer 2: Column-Level Masking** | What data users see in sensitive columns | Data contract + `sensitive_access` | Unity Catalog column masks |

### Example Flow

```
User Query: SELECT email, salary FROM employee_table
                    ↓
┌─────────────────────────────────────────────────┐
│ Layer 1: Table Access Check                     │
│ ✓ User has SELECT on employee_table             │
└─────────────────────────────────────────────────┘
                    ↓
┌─────────────────────────────────────────────────┐
│ Layer 2: Column Masking Check                   │
│ • email (privacy: pii)                          │
│   → User's role has pii in sensitive_access?    │
│   → YES: Show raw email                         │
│                                                  │
│ • salary (privacy: financial_pii)               │
│   → User's role has financial_pii?              │
│   → NO: Show hashed salary                      │
└─────────────────────────────────────────────────┘
                    ↓
Result: user@example.com, 5d41402abc4b2a76...
        (raw email)       (masked salary)
```

### What Makes This Different

Traditional approach:
- ❌ Some users get access, others get "access denied"
- ❌ Hard to give partial access to sensitive tables
- ❌ Either all-or-nothing

NovaFlow approach:
- ✅ Everyone with table access gets data back
- ✅ Sensitive columns automatically masked based on role
- ✅ Granular control at column sensitivity level

---

## Defining Privacy in Data Contracts {#defining-privacy-in-contracts}

### Where Privacy is Defined

Privacy requirements are defined in the **data contract** for each table, specifically in the `schema.properties` section where you define each column.

### Privacy Fields

For each column, you can specify:

| Field | Required? | Purpose | Example Values |
|-------|-----------|---------|----------------|
| `privacy` | ✅ Yes | Sensitivity classification | `pii`, `quasi_pii`, `special`, `financial_pii`, etc. |
| `maskingStrategy` | ⚠️ Optional | Override default masking | `hash`, `redact`, `partial`, `mask_email` |

### The 13 Sensitivity Classifications

NovaFlow supports 13 sensitivity categories aligned with GDPR and industry standards:

<details>
<summary><strong>📊 Click to expand: Full sensitivity classification table</strong></summary>

| Classification | GDPR Category | Description | Examples | Default Masking |
|----------------|---------------|-------------|----------|-----------------|
| **`pii`** | Art. 4 | Basic Personal Data (Direct identifiers) | first_name, last_name, email, phone | Hash (SHA-256) |
| **`quasi_pii`** | Art. 4 | Quasi-identifiers (Indirect identifiers) | postcode, age_band, gender, job_title | Partial masking |
| **`special`** | **Art. 9** | Special Category Personal Data | religion, ethnicity, health_condition | **Redact** (highest protection) |
| **`criminal`** | **Art. 10** | Criminal Offence Data | criminal_record, conviction_date | **Redact** |
| **`child`** | Art. 8 | Children's Personal Data | child_name, child_dob, guardian_id | **Redact** |
| **`financial_pii`** | Financial Regs | Financial Personal Data | bank_account, salary, tax_id | Hash |
| **`pci`** | PCI DSS | Payment Card Data | pan, cvv, cardholder_name | **Redact** (never expose) |
| **`auth`** | Security | Authentication & Security Data | password_hash, api_key, oauth_token | **Redact** (never expose) |
| **`location`** | Contextual | Precise Location Data | gps_latitude, home_coordinates | Partial |
| **`tracking`** | Contextual | Online Identifiers / Tracking Data | ip_address, cookie_id, device_id | Hash |
| **`hr`** | Risk-based | Employment / HR Data | performance_rating, disciplinary_action | Hash |
| **`commercial`** | Non-personal | Commercially Sensitive Financial Data | revenue, forecast, margin | None (access control only) |
| **`ip`** | Non-personal | Intellectual Property / Trade Secrets | algorithm_details, model_weights | None (access control only) |

</details>

### Example: Data Contract with Privacy

```yaml
apiVersion: v3.0.2
kind: DataContract
name: data.finance.employee_salary
schema:
  name: bronze_finance
  table: employee_salary
  properties:
    # Non-sensitive column
    - name: EMPLOYEE_ID
      type: bigint
      privacy: none          # ← Not sensitive
      maskingStrategy: none

    # Quasi-identifier (indirect identifier)
    - name: POSTCODE
      type: string
      privacy: quasi_pii     # ← Can identify when combined with other data
      maskingStrategy: mask_postcode  # Custom: SW1A 2AA → SW1A ***

    # Basic PII (direct identifier)
    - name: EMAIL
      type: string
      privacy: pii           # ← Direct identifier
      maskingStrategy: mask_email     # Custom: user@example.com → us***@example.com

    # Financial PII
    - name: SALARY
      type: decimal(18,2)
      privacy: financial_pii # ← Financial personal data
      maskingStrategy: hash  # SHA-256 hashing

    # Special category data (GDPR Art. 9)
    - name: DISABILITY_FLAG
      type: boolean
      privacy: special       # ← Highest protection level
      maskingStrategy: redact # Full redaction (REDACTED)
```

> ℹ️ **Info**: If you don't specify `maskingStrategy`, NovaFlow uses the default for that `privacy` classification (see table above).

> ⚠️ **Warning**: Columns marked as `privacy: none` will NOT be masked, even if they contain sensitive-looking data. Ensure all sensitive columns are properly classified.

---

## Defining Sensitivity Access in Roles {#defining-sensitivity-access}

### Where Sensitivity Access is Defined

Sensitivity access is defined in **domain role files**:

**File**: `/Volumes/{catalog}/contract_registry/access/domains/{domain}/domain.roles.yaml`

### The `sensitive_access` Field

Each domain role defines a `sensitive_access` list—the sensitivity levels this role can see **unmasked**.

```yaml
domain: bronze_finance

roles:
  analyst.general:
    inherits: analyst
    scope:
      exclude: [highly_sensitive_table]
    sensitive_access: [quasi_pii]  # ← Can only see quasi-identifiers unmasked
    description: "General analysts - limited sensitive data access"

  analyst.sensitive:
    inherits: analyst
    scope:
      exclude: [highly_sensitive_table]
    sensitive_access: [pii, quasi_pii]  # ← Can see PII + quasi-identifiers
    description: "Analysts with PII access"

  finance.specialist:
    inherits: finance-specialist
    scope:
      include: [employee_salary, employee_compensation]
    sensitive_access: [pii, financial_pii]  # ← Can see financial data
    description: "Finance team - salary data access"

  hr.specialist:
    inherits: analyst
    scope:
      include: [employee_records]
    sensitive_access: [pii, hr, special]  # ← Can see HR + special category
    description: "HR team - employee data including special category"

  compliance.officer:
    inherits: analyst
    scope:
      include: []  # All tables
    sensitive_access: [pii, quasi_pii, special, criminal, financial_pii, hr]
    description: "Compliance officers - full access for audits"
```

### How `sensitive_access` Works

| If role has... | Then user sees... |
|----------------|-------------------|
| `[quasi_pii]` | ✅ Quasi-identifiers unmasked<br>🔒 Everything else masked |
| `[pii, quasi_pii]` | ✅ PII + quasi-identifiers unmasked<br>🔒 Everything else masked |
| `[pii, financial_pii]` | ✅ PII + financial data unmasked<br>🔒 Everything else masked |
| `[none]` or `[]` | 🔒 Everything masked (no sensitive data access) |
| `[pii, quasi_pii, special, criminal, ...]` | ✅ Full access to all listed categories |

> 💡 **Key Point**: Users must have BOTH table access (via `scope`) AND the sensitivity level in their `sensitive_access` list to see data unmasked.

### Mapping Roles to AD Groups

Roles are mapped to AD groups in `domain.mappings.{env}.yaml`:

```yaml
domain: bronze_finance

mappings:
  analyst.general:
    ad_groups:
      - CLUK-CAZ-EDP-dev-data-analysts-general

  analyst.sensitive:
    ad_groups:
      - CLUK-CAZ-EDP-dev-data-analysts-sensitive
      - CLUK-CAZ-EDP-dev-customer-insights-team

  finance.specialist:
    ad_groups:
      - CLUK-CAZ-EDP-dev-customer-services-finance-advisors
```

---

## What Happens Automatically {#what-happens-automatically}

### NovaFlow's Automatic Privacy Enforcement

Once you define privacy in contracts and sensitivity access in roles, NovaFlow handles everything else:

#### Step 1: Privacy Analysis
- NovaFlow reads the data contract
- Extracts all columns with `privacy` classifications
- Identifies which columns need masking

#### Step 2: Role Resolution
- Reads domain roles and mappings
- Determines which AD groups can see which sensitivity levels
- Builds exempt group lists per column

#### Step 3: Differential Enforcement
- Queries Unity Catalog for current masking state
- Compares intended vs actual masking
- Calculates what needs to change (CREATE/DROP)

#### Step 4: UC Masking Application
- Generates role-based SQL masking expressions
- Applies to Unity Catalog using `ALTER TABLE ... SET MASK`
- Only changes what's needed (idempotent)

#### Step 5: Audit Logging
- Logs all masking changes
- Records which groups have access
- Tracks enforcement metrics

### When Enforcement Happens

Privacy enforcement occurs automatically:

| Trigger | What Happens |
|---------|--------------|
| **Pipeline Execution** | `PrivacyStage` runs after data is written |
| **On-Demand** | Data steward runs standalone tool |
| **Contract Update** | Re-run privacy enforcement for affected tables |
| **Role Update** | Re-run privacy enforcement for affected domains |

### Example: Generated SQL

For a column with `privacy: pii`:

```sql
ALTER TABLE bronze_finance.employee_salary
ALTER COLUMN EMAIL
SET MASK CASE
    -- Groups with 'pii' in their sensitive_access
    WHEN is_account_group_member('CLUK-CAZ-EDP-dev-data-analysts-sensitive') THEN EMAIL
    WHEN is_account_group_member('CLUK-CAZ-EDP-dev-customer-insights-team') THEN EMAIL
    WHEN is_account_group_member('CLUK-CAZ-EDP-dev-customer-services-finance-advisors') THEN EMAIL
    -- All other groups see masked data
    ELSE CASE
        WHEN EMAIL IS NOT NULL AND INSTR(EMAIL, '@') > 0 THEN
            CONCAT(SUBSTRING(EMAIL, 1, 2), '***@', SUBSTRING(EMAIL, INSTR(EMAIL, '@') + 1))
        ELSE NULL
    END
END
```

> 📝 **Note**: This SQL is generated automatically. You never write or maintain it.

---

## Examples {#examples}

### Example 1: Employee Salary Table

**Scenario**: HR needs full employee data. Finance needs salary data. General analysts need demographics only.

#### Data Contract
```yaml
schema:
  properties:
    - name: EMPLOYEE_ID
      privacy: none

    - name: AGE_BAND
      privacy: quasi_pii  # Demographic data

    - name: EMAIL
      privacy: pii  # Direct identifier

    - name: SALARY
      privacy: financial_pii  # Financial data

    - name: DISABILITY_FLAG
      privacy: special  # Special category (Art. 9)
```

#### Domain Roles
```yaml
roles:
  analyst.general:
    sensitive_access: [quasi_pii]

  finance.specialist:
    sensitive_access: [pii, financial_pii]

  hr.specialist:
    sensitive_access: [pii, hr, special]
```

#### What Each Role Sees

| Column | analyst.general | finance.specialist | hr.specialist |
|--------|-----------------|-------------------|---------------|
| EMPLOYEE_ID | ✅ 12345 | ✅ 12345 | ✅ 12345 |
| AGE_BAND | ✅ 25-30 | 🔒 ****30 | 🔒 ****30 |
| EMAIL | 🔒 e3b0c442... | ✅ john@example.com | ✅ john@example.com |
| SALARY | 🔒 5d414023... | ✅ 85000.00 | 🔒 5d414023... |
| DISABILITY_FLAG | 🔒 REDACTED | 🔒 REDACTED | ✅ true |

---

### Example 2: Customer Contact Data

**Scenario**: Marketing needs email/phone. Sales needs full contact details. Everyone else sees aggregates only.

<details>
<summary><strong>📄 Click to expand: Full configuration</strong></summary>

**Data Contract**:
```yaml
schema:
  properties:
    - name: CUSTOMER_ID
      privacy: none

    - name: POSTCODE_AREA
      privacy: quasi_pii

    - name: EMAIL
      privacy: pii

    - name: PHONE
      privacy: pii

    - name: PURCHASE_HISTORY
      privacy: none
```

**Domain Roles**:
```yaml
roles:
  analyst.general:
    sensitive_access: []  # No sensitive data

  marketing.specialist:
    sensitive_access: [pii]  # Email, phone

  sales.representative:
    sensitive_access: [pii, quasi_pii]  # Full contact details
```

**Result**:
- Analysts: See CUSTOMER_ID and PURCHASE_HISTORY, all PII masked
- Marketing: See PII (email, phone), quasi-identifiers masked
- Sales: See everything unmasked

</details>

---

## FAQs {#faqs}

<details>
<summary><strong>Q: What happens if I don't specify `privacy` for a column?</strong></summary>

**A**: The column defaults to `privacy: none`, which means no masking is applied. The column is accessible to everyone with table access.

⚠️ **Important**: Always explicitly mark sensitive columns with the appropriate privacy classification.
</details>

<details>
<summary><strong>Q: Can I override the default masking strategy?</strong></summary>

**A**: Yes, use the `maskingStrategy` field in the data contract:

```yaml
- name: EMAIL
  privacy: pii
  maskingStrategy: mask_email  # Override default (hash) with custom email masking
```
</details>

<details>
<summary><strong>Q: What if a user needs access to multiple sensitivity levels?</strong></summary>

**A**: List multiple classifications in `sensitive_access`:

```yaml
analyst.senior:
  sensitive_access: [pii, quasi_pii, financial_pii]
```

The user will see all three sensitivity levels unmasked.
</details>

<details>
<summary><strong>Q: How do I give someone access to ALL sensitive data?</strong></summary>

**A**: List all sensitivity levels in their role:

```yaml
compliance.officer:
  sensitive_access: [pii, quasi_pii, special, criminal, child, financial_pii, pci, auth, location, tracking, hr]
```

Or use a dedicated admin role with broad access.
</details>

<details>
<summary><strong>Q: Can I see what masking is currently applied to a table?</strong></summary>

**A**: Yes, query Unity Catalog:

```sql
DESCRIBE TABLE bronze_finance.employee_salary
```

Or use the NovaFlow standalone tool:

```python
tool.preview_privacy_changes(contract, catalog)
```
</details>

<details>
<summary><strong>Q: What happens when I update a data contract or role definition?</strong></summary>

**A**: You must re-run privacy enforcement:

```python
# Via standalone tool
tool.apply_privacy_to_table(contract, catalog)

# Or via pipeline (automatic on next run)
```

NovaFlow detects the changes and updates Unity Catalog accordingly.
</details>

<details>
<summary><strong>Q: Do users see "access denied" errors?</strong></summary>

**A**: No! This is the key difference. Users with table access always get data back, but sensitive columns are automatically masked if they don't have the appropriate role.

Only users without table access (no SELECT grant) see "access denied."
</details>

<details>
<summary><strong>Q: How does this work with data exports or downstream systems?</strong></summary>

**A**: Unity Catalog masking applies at query time. If a user exports data (CSV, Parquet, etc.), the exported data reflects what they see—masked columns remain masked in the export.

This ensures consistent protection regardless of how data is accessed.
</details>

---

## Getting Help {#getting-help}

### Documentation
- [Engineering Standards Checklist](docs/ENGINEERING_STANDARDS_CHECKLIST.md)
- [Privacy Examples](samples/privacy_examples/README.md)
- [Why Framework vs Documentation](docs/WHY_FRAMEWORK_NOT_DOCUMENTATION.md)

### Support Channels
- **Slack**: #novaflow-support
- **Email**: novaflow-team@company.com
- **Office Hours**: Tuesdays & Thursdays 2-3pm GMT

### Common Tasks
- [How to add privacy to an existing table](#)
- [How to create a new domain role](#)
- [How to audit current masking](#)
- [How to test privacy before production](#)

---

**Questions or feedback?** Comment below or reach out to the NovaFlow team.

---

_This is a living document. Last updated: January 2026_
