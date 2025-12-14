# Data Quality Rules Reference

**Version:** 1.0
**Last Updated:** 2025-12-14
**Component:** NovaFlow DQEngine

---

## Overview

The DQEngine provides a comprehensive set of data quality rules for cleansing (transforming) and validating data in your pipelines. Rules are defined in YAML data contracts and executed automatically during pipeline runs.

### Rule Types

- **Cleansing Rules** - Transform and standardize data (e.g., trim, uppercase, remove accents)
- **Validation Rules** - Check data quality and generate quality scores (e.g., not_null, is_email, min/max)

---

## Quick Start

### Basic Contract Structure

```yaml
# Data Contract YAML
cleansing_rules:
  - rule: trim
    columns: [name, email]
  - rule: upper
    column: country_code

quality_rules:
  - rule: not_null
    column: customer_id
    weight: 2
  - rule: is_email
    columns: [email, alternate_email]
    weight: 1
```

### How It Works

1. **Cleansing runs first** - Data is transformed in-place
2. **Validation runs second** - Quality score (0-100) is calculated
3. **Results are tracked** - DQ errors logged to violations table
4. **Data flows forward** - Even "bad" rows continue through pipeline (with low dq_score)

---

## Cleansing Rules

Cleansing rules modify data to standardize and clean values.

### String Cleaning

#### trim
Remove leading/trailing whitespace.

```yaml
- rule: trim
  columns: [name, address, email]
```

#### upper
Convert to uppercase.

```yaml
- rule: upper
  columns: [country_code, state_code]
```

#### lower
Convert to lowercase.

```yaml
- rule: lower
  columns: [email, username]
```

#### title_case
Convert to Title Case (first letter of each word capitalized).

```yaml
- rule: title_case
  columns: [first_name, last_name, city]
```

**Example:** `"john doe"` → `"John Doe"`

---

### String Length Management

#### truncate
Truncate strings to maximum length.

```yaml
- rule: truncate
  column: description
  max_length: 500
```

**Example:** Truncates to first 500 characters

#### pad_left
Left-pad strings to fixed length.

```yaml
- rule: pad_left
  column: account_id
  length: 10
  pad_char: "0"  # default: "0"
```

**Example:** `"123"` → `"0000000123"`

**Use Cases:**
- Zero-padding account numbers
- Fixed-width file formatting
- Code standardization

#### pad_right
Right-pad strings to fixed length.

```yaml
- rule: pad_right
  column: code
  length: 20
  pad_char: " "  # default: " " (space)
```

**Example:** `"ABC"` → `"ABC                 "`

---

### Character Manipulation

#### remove_special_characters
Remove special characters, optionally keeping specific ones.

```yaml
- rule: remove_special_characters
  columns: [name, address]
  keep_chars: ".-"  # Keep hyphens and periods
```

**Example:** `"John@Doe#123"` → `"JohnDoe123"` (if keep_chars="")
**Example:** `"John-Doe.Jr"` → `"John-Doe.Jr"` (if keep_chars=".-")

**Use Cases:**
- Sanitizing user input
- Normalizing names while preserving hyphens
- Cleaning phone numbers while keeping dashes

#### remove_accents
Remove accents from international characters.

```yaml
- rule: remove_accents
  columns: [name, address, city]
```

**Example:**
- `"José"` → `"Jose"`
- `"François"` → `"Francois"`
- `"São Paulo"` → `"Sao Paulo"`

**Use Cases:**
- Normalizing international names
- Deduplication across character sets
- ASCII-only system compatibility

---

### Regex Operations

#### regex_replace
Replace pattern with replacement string.

```yaml
- rule: regex_replace
  column: phone
  pattern: "[^0-9]"
  replacement: ""  # Remove all non-digits
```

**Example:** `"(555) 123-4567"` → `"5551234567"`

#### extract_regex
Extract substring matching regex pattern.

```yaml
- rule: extract_regex
  column: phone
  pattern: '\((\d{3})\)'  # Extract area code
  group: 1
```

**Example:** `"(555) 123-4567"` → `"555"`

**Common Patterns:**
- Area code: `'\((\d{3})\)'`, group=1
- Domain from email: `'@([\w.-]+)'`, group=1
- Year from date: `'(\d{4})'`, group=1

---

### Data Standardization

#### standardize_email
Lowercase and trim emails.

```yaml
- rule: standardize_email
  columns: [email, alternate_email]
```

**Example:** `"  John.Doe@EXAMPLE.COM  "` → `"john.doe@example.com"`

**Use Cases:**
- Email deduplication
- Case-insensitive matching
- Data normalization

#### normalize_boolean_values
Map truthy/falsy values to "True"/"False".

```yaml
- rule: normalize_boolean_values
  column: is_active
```

**Mapping:**
- `"1", "t", "true", "yes", "y"` → `"True"`
- `"0", "f", "false", "no", "n"` → `"False"`
- All other values → unchanged

---

### Null Handling

#### nullify_empty_strings
Convert empty strings to NULL.

```yaml
- rule: nullify_empty_strings
  columns: [optional_field, notes]
```

**Example:** `""` → `NULL`

#### replace_nulls
Replace NULL with default value.

```yaml
- rule: replace_nulls
  column: status
  default_value: "UNKNOWN"
```

**Example:** `NULL` → `"UNKNOWN"`

---

### Numeric Operations

#### round_numeric
Round to specified decimal places.

```yaml
- rule: round_numeric
  column: price
  decimal_places: 2  # default: 2
```

**Example:** `12.3456` → `12.35`

---

## Validation Rules

Validation rules check data quality without modifying values. Failed validations lower the `dq_score` (0-100).

### Understanding Weights

```yaml
quality_rules:
  - rule: not_null
    column: customer_id
    weight: 3  # Critical - worth 3 points

  - rule: is_email
    column: email
    weight: 1  # Nice-to-have - worth 1 point
```

**Total weight:** 4
**If customer_id is NULL:** `dq_score = (4-3)/4 * 100 = 25%`
**If email is invalid:** `dq_score = (4-1)/4 * 100 = 75%`

---

### Null/Blank Checks

#### not_null
Value must not be NULL.

```yaml
- rule: not_null
  column: customer_id
  weight: 2
```

#### not_blank
Value must not be NULL or empty string (after trimming).

```yaml
- rule: not_blank
  column: email
  weight: 2
```

**Difference:**
- `not_null`: Only checks for NULL
- `not_blank`: Checks for NULL or `""` (whitespace-only also fails)

---

### String Length Validation

#### min_length
Minimum string length.

```yaml
- rule: min_length
  column: password
  value: 8
  weight: 2
```

**Fails if:** `len(password) < 8`

#### max_length
Maximum string length.

```yaml
- rule: max_length
  column: username
  value: 50
  weight: 1
```

**Fails if:** `len(username) > 50`

#### length_equals
Exact string length (for fixed-length codes).

```yaml
- rule: length_equals
  columns: [country_code, state_code]
  length: 2
  weight: 1
```

**Fails if:** `len(country_code) != 2`

**Use Cases:**
- ISO country codes (2 chars)
- State codes (2 chars)
- Fixed-length IDs

---

### Format Validation

#### is_email
Valid email format.

```yaml
- rule: is_email
  columns: [email, alternate_email]
  weight: 1
```

**Pattern:** `^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$`

**Valid:** `john.doe@example.com`
**Invalid:** `john.doe@`, `@example.com`, `invalid.email`

#### is_url
Valid URL format (http/https).

```yaml
- rule: is_url
  column: website
  weight: 1
```

**Pattern:** `^https?://[^\s/$.?#].[^\s]*$`

**Valid:** `https://example.com`, `http://sub.example.com/path`
**Invalid:** `example.com`, `ftp://example.com`

#### is_uuid
Valid UUID format (8-4-4-4-12).

```yaml
- rule: is_uuid
  column: transaction_id
  weight: 1
```

**Pattern:** `^[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}$`

**Valid:** `550e8400-e29b-41d4-a716-446655440000`

#### is_valid_json
Valid JSON format.

```yaml
- rule: is_valid_json
  column: metadata
  weight: 1
```

**Valid:** `{"key": "value"}`, `["a", "b", "c"]`
**Invalid:** `{invalid}`, `not json`

---

### Numeric Validation

#### is_number
Value can be converted to number.

```yaml
- rule: is_number
  column: quantity
  weight: 1
```

**Valid:** `"123"`, `"45.67"`, `"-89"`
**Invalid:** `"abc"`, `"12.34.56"`

#### min
Minimum numeric value.

```yaml
- rule: min
  column: age
  value: 0
  weight: 1
```

**Fails if:** `age < 0`

#### max
Maximum numeric value.

```yaml
- rule: max
  column: discount_pct
  value: 100
  weight: 1
```

**Fails if:** `discount_pct > 100`

#### between
Value within numeric range.

```yaml
- rule: between
  column: rating
  min: 1
  max: 5
  weight: 1
```

**Fails if:** `rating < 1 OR rating > 5`

---

### Numeric Sign Validation

#### is_positive
Value must be positive (> 0).

```yaml
- rule: is_positive
  columns: [quantity, price]
  weight: 1
```

**Fails if:** `value <= 0`

#### is_negative
Value must be negative (< 0).

```yaml
- rule: is_negative
  column: adjustment
  weight: 1
```

**Fails if:** `value >= 0`

#### is_non_negative
Value must be non-negative (>= 0).

```yaml
- rule: is_non_negative
  columns: [age, balance]
  weight: 1
```

**Fails if:** `value < 0`

**Use Cases:**
- Age must be >= 0
- Quantity must be > 0
- Balance adjustments can be negative

---

### Pattern Matching

#### regex
Match regex pattern.

```yaml
- rule: regex
  column: phone
  pattern: '^\d{3}-\d{3}-\d{4}$'
  weight: 1
```

**Valid:** `555-123-4567`
**Invalid:** `5551234567`, `(555) 123-4567`

#### allowed_values
Value must be in allowed list.

```yaml
- rule: allowed_values
  column: status
  values: ["ACTIVE", "INACTIVE", "PENDING"]
  weight: 1
```

**Fails if:** `status NOT IN ("ACTIVE", "INACTIVE", "PENDING")`

#### not_in_list
Value must NOT be in forbidden list (blacklist).

```yaml
- rule: not_in_list
  column: status
  forbidden_values: ["DELETED", "ARCHIVED", "PURGED"]
  weight: 2
```

**Fails if:** `status IN ("DELETED", "ARCHIVED", "PURGED")`

---

### String Pattern Validation

#### starts_with
String must start with prefix.

```yaml
- rule: starts_with
  column: account_id
  prefix: "ACC-"
  weight: 1
```

**Valid:** `"ACC-12345"`
**Invalid:** `"12345"`, `"USR-12345"`

#### ends_with
String must end with suffix.

```yaml
- rule: ends_with
  column: filename
  suffix: ".pdf"
  weight: 1
```

**Valid:** `"document.pdf"`
**Invalid:** `"document.txt"`

#### contains
String must contain substring.

```yaml
- rule: contains
  column: description
  substring: "URGENT"
  weight: 2
```

**Valid:** `"URGENT: System down"`
**Invalid:** `"System maintenance"`

#### not_contains
String must NOT contain substring (blacklist).

```yaml
- rule: not_contains
  column: comment
  substring: "REDACTED"
  weight: 1
```

**Valid:** `"Customer feedback"`
**Fails if:** `"REDACTED information"`

---

### Date Validation

#### is_date
Valid date format.

```yaml
- rule: is_date
  column: birth_date
  format: "yyyy-MM-dd"  # default
  weight: 1
```

**Valid:** `"2000-01-15"`
**Invalid:** `"2000-13-45"`, `"not a date"`

#### is_future_date
Date must be in the future.

```yaml
- rule: is_future_date
  column: expiry_date
  format: "yyyy-MM-dd"
  weight: 1
```

**Fails if:** `expiry_date <= current_date`

**Use Cases:**
- Credit card expiry
- License expiration
- Warranty end dates

#### is_past_date
Date must be in the past.

```yaml
- rule: is_past_date
  column: birth_date
  format: "yyyy-MM-dd"
  weight: 1
```

**Fails if:** `birth_date >= current_date`

**Use Cases:**
- Birth dates
- Historical transaction dates
- Completion dates

#### date_range
Date must be within range.

```yaml
- rule: date_range
  column: transaction_date
  min_date: "2020-01-01"
  max_date: "2025-12-31"
  format: "yyyy-MM-dd"
  weight: 1
```

**Fails if:** `transaction_date < '2020-01-01' OR transaction_date > '2025-12-31'`

---

### Cross-Field Validation

#### greater_than_column
Column must be greater than reference column.

```yaml
- rule: greater_than_column
  column: end_date
  reference_column: start_date
  weight: 3
```

**Fails if:** `end_date <= start_date`

**Use Cases:**
- End date > Start date
- Max value > Min value
- Sale price > Cost

#### less_than_column
Column must be less than reference column.

```yaml
- rule: less_than_column
  column: discount
  reference_column: price
  weight: 2
```

**Fails if:** `discount >= price`

#### equals_column
Column must equal reference column.

```yaml
- rule: equals_column
  column: email_confirmation
  reference_column: email
  weight: 2
```

**Fails if:** `email_confirmation != email`

**Use Cases:**
- Email confirmation matching
- Password confirmation
- Duplicate field verification

---

### Uniqueness Validation

#### unique
Value must be unique across all rows.

```yaml
- rule: unique
  column: customer_id
  weight: 3
```

**Fails if:** Duplicate `customer_id` values exist

#### composite_unique
Combination of columns must be unique.

```yaml
- rule: composite_unique
  columns: [customer_id, order_date, product_id]
  weight: 3
```

**Fails if:** Duplicate combinations exist

**Use Cases:**
- Natural key validation
- Preventing duplicate transactions
- Ensuring unique combinations

---

## Common Patterns

### Email Standardization + Validation

```yaml
cleansing_rules:
  - rule: trim
    columns: [email, alternate_email]
  - rule: standardize_email
    columns: [email, alternate_email]

quality_rules:
  - rule: not_blank
    column: email
    weight: 2
  - rule: is_email
    columns: [email, alternate_email]
    weight: 1
```

### Name Standardization

```yaml
cleansing_rules:
  - rule: trim
    columns: [first_name, last_name]
  - rule: title_case
    columns: [first_name, last_name]
  - rule: remove_accents
    columns: [first_name, last_name]

quality_rules:
  - rule: not_blank
    columns: [first_name, last_name]
    weight: 2
```

### Fixed-Length Code Validation

```yaml
cleansing_rules:
  - rule: upper
    column: country_code
  - rule: trim
    column: country_code

quality_rules:
  - rule: not_null
    column: country_code
    weight: 2
  - rule: length_equals
    column: country_code
    length: 2
    weight: 1
  - rule: allowed_values
    column: country_code
    values: ["US", "CA", "MX", "GB", "FR", "DE"]
    weight: 1
```

### Date Range Validation

```yaml
quality_rules:
  - rule: not_null
    columns: [start_date, end_date]
    weight: 2
  - rule: is_date
    columns: [start_date, end_date]
    format: "yyyy-MM-dd"
    weight: 1
  - rule: greater_than_column
    column: end_date
    reference_column: start_date
    weight: 3
```

### Phone Number Cleaning

```yaml
cleansing_rules:
  - rule: regex_replace
    column: phone
    pattern: "[^0-9]"
    replacement: ""
  - rule: pad_left
    column: phone
    length: 10
    pad_char: "0"

quality_rules:
  - rule: length_equals
    column: phone
    length: 10
    weight: 1
  - rule: digits_only
    column: phone
    weight: 1
```

### Amount Validation

```yaml
quality_rules:
  - rule: not_null
    column: amount
    weight: 2
  - rule: is_number
    column: amount
    weight: 1
  - rule: is_non_negative
    column: amount
    weight: 1
  - rule: max
    column: amount
    value: 999999.99
    weight: 1
```

---

## Best Practices

### Rule Ordering

1. **Cleansing first, validation second** - Always clean before validating
2. **Trim before formatting** - Remove whitespace before other operations
3. **Case conversion before matching** - Standardize case before pattern matching

### Weight Guidelines

| Priority | Weight | Use Case |
|----------|--------|----------|
| Critical | 3-5 | Primary keys, required fields, business-critical |
| Important | 2 | Secondary keys, important business logic |
| Nice-to-have | 1 | Optional fields, cosmetic validation |

### Performance Tips

1. **Use `columns` for bulk operations** - More efficient than multiple rules
   ```yaml
   # Good
   - rule: trim
     columns: [col1, col2, col3, col4]

   # Bad
   - rule: trim
     column: col1
   - rule: trim
     column: col2
   ```

2. **Order rules by cost** - Cheap validations first (not_null) before expensive ones (regex)

3. **Limit validation rules** - Each rule adds processing time

### Error Handling

- **Invalid rows are NOT dropped** - They continue with low `dq_score`
- **DQ errors are logged** - Check `dq_errors_table` for violations
- **Quarantine for read failures** - Bad rows from file read go to quarantine
- **Monitor dq_score** - Use in downstream filters: `WHERE dq_score >= 80`

---

## Troubleshooting

### Rule Not Applied

**Check:**
1. Rule name spelling (case-sensitive)
2. Required parameters present
3. Column names match schema
4. Rule is in correct section (cleansing vs quality)

### Low DQ Scores

**Debug:**
1. Query `dq_errors_table` for specific failures
2. Check `_dq_failures` column in output data
3. Review rule weights - are they appropriate?
4. Sample data to see actual vs expected

### Schema Drift

**Issue:** Table has different columns after adding rules

**Solution:**
- DQ columns (`dq_score`, `_dq_failures`, etc.) are **always added**
- This is intentional to prevent schema drift
- Use `mergeSchema=true` if needed (automatic in SCD2)

---

## Reference

### All Cleansing Rules

| Rule | Purpose | Key Parameters |
|------|---------|----------------|
| trim | Remove whitespace | columns |
| upper | Convert to uppercase | columns |
| lower | Convert to lowercase | columns |
| title_case | Title Case | columns |
| truncate | Truncate to max length | column, max_length |
| pad_left | Left-pad to length | column, length, pad_char |
| pad_right | Right-pad to length | column, length, pad_char |
| remove_special_characters | Remove special chars | columns, keep_chars |
| remove_accents | Remove accents | columns |
| regex_replace | Replace pattern | column, pattern, replacement |
| extract_regex | Extract pattern | column, pattern, group |
| standardize_email | Lowercase + trim email | columns |
| normalize_boolean_values | Map to True/False | column |
| nullify_empty_strings | Empty → NULL | columns |
| replace_nulls | NULL → default | column, default_value |
| round_numeric | Round decimals | column, decimal_places |

### All Validation Rules

| Rule | Purpose | Key Parameters |
|------|---------|----------------|
| not_null | Not NULL | column, weight |
| not_blank | Not NULL or empty | column, weight |
| min_length | Min string length | column, value, weight |
| max_length | Max string length | column, value, weight |
| length_equals | Exact length | columns, length, weight |
| is_email | Email format | columns, weight |
| is_url | URL format | columns, weight |
| is_uuid | UUID format | columns, weight |
| is_valid_json | JSON format | columns, weight |
| is_number | Numeric | column, weight |
| min | Min numeric value | column, value, weight |
| max | Max numeric value | column, value, weight |
| between | Numeric range | column, min, max, weight |
| is_positive | Positive (> 0) | columns, weight |
| is_negative | Negative (< 0) | columns, weight |
| is_non_negative | Non-negative (>= 0) | columns, weight |
| regex | Regex pattern | column, pattern, weight |
| allowed_values | Whitelist | column, values, weight |
| not_in_list | Blacklist | columns, forbidden_values, weight |
| starts_with | Prefix match | columns, prefix, weight |
| ends_with | Suffix match | columns, suffix, weight |
| contains | Contains substring | columns, substring, weight |
| not_contains | Not contains substring | columns, substring, weight |
| is_date | Date format | column, format, weight |
| is_future_date | Future date | columns, format, weight |
| is_past_date | Past date | columns, format, weight |
| date_range | Date within range | column, min_date, max_date, format, weight |
| greater_than_column | Column > reference | column, reference_column, weight |
| less_than_column | Column < reference | column, reference_column, weight |
| equals_column | Column = reference | column, reference_column, weight |
| unique | Unique values | column, weight |
| composite_unique | Unique combination | columns, weight |
| digits_only | Only digits | column, weight |
| letters_only | Only letters | column, weight |
| conditional | Custom SQL expression | expression, weight |

---

## Support

- **Documentation:** `/docs/DQ_Rules_Reference.md`
- **Examples:** `/samples/data.*.yaml`
- **Source Code:** `nova_framework/quality/dq.py`
- **Issues:** Contact data platform team
