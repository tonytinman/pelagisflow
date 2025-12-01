# NovaFlow Data Access Control Implementation

Complete implementation of table-level differential access control for Unity Catalog.

## 📁 Files Structure

```
access/                              # Domain module (business logic)
├── __init__.py                      # Module exports
├── models.py                        # Data models
├── metadata_loader.py               # Load metadata from registry
├── uc_inspector.py                  # Query UC privileges
├── delta_generator.py               # Calculate deltas
├── grant_revoker.py                 # Execute GRANT/REVOKE
└── standalone.py                    # Standalone tool

pipeline/stages/
└── access_control_stage.py          # Pipeline integration (thin adapter)
```

## 🚀 Installation

### 1. Copy Files to NovaFlow

```bash
# Copy access module
cp -r access/ /path/to/pelagisflow/

# Copy pipeline stage
cp pipeline/stages/access_control_stage.py /path/to/pelagisflow/pipeline/stages/
```

### 2. Update Pipeline Strategy

```python
# In pipeline/strategies/ingestion.py

from pelagisflow.pipeline.stages.access_control_stage import AccessControlStage

class IngestionPipeline(BasePipeline):
    def build_stages(self) -> List[AbstractStage]:
        stages = [
            ReadStage(self.context, self.stats, reader_type="file"),
            LineageStage(self.context, self.stats),
            HashingStage(self.context, self.stats),
            DeduplicationStage(self.context, self.stats),
            QualityStage(self.context, self.stats),
            WriteStage(self.context, self.stats)
        ]
        
        # Add access control if enabled
        if self._access_control_enabled():
            environment = self._get_environment()
            stages.append(AccessControlStage(
                self.context,
                self.stats,
                environment=environment
            ))
        
        return stages
    
    def _access_control_enabled(self) -> bool:
        """Check if access control enabled."""
        return self.context.contract.get("accessControl.enabled", True)
    
    def _get_environment(self) -> str:
        """Get environment from config."""
        from pelagisflow.core.config import get_config
        config = get_config()
        return config.environment
```

### 3. Update Config

```python
# In core/config.py

@dataclass
class FrameworkConfig:
    # Add environment field
    environment: str = "dev"  # dev, test, prod
    
    # ... existing fields ...
    
    @staticmethod
    def from_env():
        """Load from environment variables."""
        return FrameworkConfig(
            environment=os.getenv("NOVA_ENVIRONMENT", "dev"),
            # ... existing fields ...
        )
```

## 📊 Metadata Structure

### Directory Layout

```
/Volumes/{catalog}/contract_registry/
├── access/                          # NEW
│   ├── global_roles.yaml            # Platform team owns
│   └── domains/
│       ├── finance/
│       │   ├── domain.roles.yaml    # Finance team owns
│       │   ├── domain.mappings.dev.yaml
│       │   ├── domain.mappings.test.yaml
│       │   └── domain.mappings.prod.yaml
│       └── hr/
│           └── (same structure)
└── contracts/
    └── (existing contracts)
```

### Example Files

#### global_roles.yaml

```yaml
roles:
  consumer:
    privileges: [SELECT]
    description: "Standard read-only access for data consumers"
  
  writer:
    privileges: [SELECT, MODIFY]
    description: "Read and write access for data producers"
  
  admin:
    privileges: [ALL PRIVILEGES]
    description: "Full administrative access to domain tables"
```

#### domain.roles.yaml (finance)

```yaml
domain: finance

roles:
  consumer:
    inherits: consumer
    scope:
      include: ["*"]
    description: "General finance data consumers"
  
  consumer.general:
    inherits: consumer
    locally_scoped: true
    scope:
      exclude:
        - payment_table1
        - payment_table2
    description: "General access excluding payment tables"
  
  consumer.payments:
    inherits: consumer
    locally_scoped: true
    scope:
      include:
        - payment_table1
        - payment_table2
    description: "Access to payment tables only"
  
  writer:
    inherits: writer
    scope:
      include: ["*"]
    description: "Finance data writers"
```

#### domain.mappings.dev.yaml (finance)

```yaml
domain: finance

mappings:
  consumer:
    ad_groups:
      - ad_grp_finance_analysts_dev
      - ad_grp_bi_users_dev
  
  consumer.general:
    ad_groups:
      - ad_grp_finance_general_dev
  
  consumer.payments:
    ad_groups:
      - ad_grp_payments_team_dev
  
  writer:
    ad_groups:
      - ad_grp_finance_etl_dev
```

## 🔧 Usage

### Automatic (Pipeline)

```python
# Set environment
import os
os.environ["NOVA_ENVIRONMENT"] = "dev"

# Run pipeline - access control applied automatically
from pelagisflow.pipeline.factory import PipelineFactory

factory = PipelineFactory()
pipeline = factory.create_pipeline_from_contract(
    contract_path="/path/to/contract.yaml",
    pipeline_type="ingestion"
)

result = pipeline.execute()

# Check access control stats
print(f"Grants applied: {result.stats.get('access_grants_succeeded')}")
print(f"Revokes applied: {result.stats.get('access_revokes_succeeded')}")
```

### On-Demand (Single Table)

```python
from pyspark.sql import SparkSession
from pelagisflow.access import StandaloneAccessControlTool

spark = SparkSession.builder.getOrCreate()

tool = StandaloneAccessControlTool(
    spark=spark,
    environment="dev",
    dry_run=False  # Set True to preview
)

# Apply to single table
result = tool.apply_to_table(
    catalog="cluk_dev_nova",
    schema="finance",
    table="payment_table1"
)

print(f"Success: {result.is_successful}")
print(f"GRANTs: {result.grants_succeeded}/{result.grants_attempted}")
print(f"REVOKEs: {result.revokes_succeeded}/{result.revokes_attempted}")
```

### On-Demand (Entire Schema)

```python
# Apply to all tables in finance schema
results = tool.apply_to_schema(
    catalog="cluk_dev_nova",
    schema="finance"
)

# Check results per table
for table, result in results.items():
    if not result.is_successful:
        print(f"Table {table} had errors:")
        for error in result.errors:
            print(f"  {error}")
```

### Audit Mode

```python
# Audit without making changes
audit_result = tool.audit_table(
    catalog="cluk_dev_nova",
    schema="finance",
    table="payment_table1"
)

print(f"Compliant: {audit_result['is_compliant']}")
print(f"GRANTs needed: {audit_result['grants_needed']}")
print(f"REVOKEs needed: {audit_result['revokes_needed']}")
```

## 📊 Observability

### Metrics Captured

Access control metrics are automatically logged to `pipeline_stats.custom_stats`:

```sql
SELECT 
  process_queue_id,
  contract_name,
  custom_stats['access_intended_count'] AS intended,
  custom_stats['access_actual_count'] AS actual,
  custom_stats['access_grants_succeeded'] AS grants,
  custom_stats['access_revokes_succeeded'] AS revokes,
  custom_stats['access_no_change_count'] AS unchanged
FROM pelagisflow.pipeline_stats
WHERE timestamp >= CURRENT_DATE - INTERVAL 7 DAYS
  AND custom_stats IS NOT NULL
ORDER BY timestamp DESC;
```

## 🧪 Testing

### Unit Tests

```python
# Test metadata loading
def test_metadata_loader():
    loader = AccessMetadataLoader(
        registry_path="/test/registry",
        environment="dev"
    )
    intents = loader.get_intended_privileges("cat", "finance", "table1")
    assert len(intents) > 0

# Test delta generation
def test_delta_generator():
    generator = PrivilegeDeltaGenerator()
    
    intents = [PrivilegeIntent(...)]
    actuals = [ActualPrivilege(...)]
    
    deltas, no_change = generator.generate_deltas(intents, actuals)
    
    assert len(deltas) > 0
```

### Integration Tests

```python
# Test end-to-end
def test_access_control_stage():
    # Setup
    context = MockExecutionContext()
    stats = MockPipelineStats()
    stage = AccessControlStage(context, stats, "dev")
    
    # Execute
    result_df = stage.execute(input_df)
    
    # Verify
    assert result_df is input_df  # Pass-through
    assert stats.get("access_grants_succeeded") >= 0
```

## 🔐 Security Considerations

### NovaFlow SP Permissions

The NovaFlow service principal needs:

```sql
-- Schema ownership (already provisioned)
GRANT OWNERSHIP ON SCHEMA {catalog}.{domain} TO `novaflow_sp`;

-- This automatically grants:
-- - ALL PRIVILEGES on schema
-- - Ability to GRANT/REVOKE table-level privileges
```

### GRANT/REVOKE Behavior

- **GRANT is idempotent**: Re-granting same privilege has no effect
- **REVOKE will error**: If privilege doesn't exist (handled gracefully)
- **Group must exist**: UC errors if AD group not SCIM-provisioned

## 📋 Deployment Checklist

### Phase 1: Setup (Week 1)
- [ ] Create `/access` directory in contract registry
- [ ] Create `global_roles.yaml`
- [ ] Create domain directories
- [ ] Copy code files to NovaFlow
- [ ] Update pipeline strategies
- [ ] Update config with environment

### Phase 2: Metadata (Week 1)
- [ ] Create `domain.roles.yaml` for each domain
- [ ] Create `domain.mappings.dev.yaml` for each domain
- [ ] Validate YAML syntax
- [ ] Ensure AD groups are SCIM-provisioned

### Phase 3: Testing (Week 2)
- [ ] Unit tests for each component
- [ ] Integration tests for stage
- [ ] Test with 1-2 pilot tables
- [ ] Verify GRANTs executed correctly
- [ ] Verify REVOKEs executed correctly
- [ ] Test dry-run mode

### Phase 4: Deployment (Week 2-3)
- [ ] Deploy to dev environment
- [ ] Run on pilot tables
- [ ] Monitor logs and stats
- [ ] Fix any issues
- [ ] Deploy to test environment
- [ ] Deploy to prod environment

## 🎯 Key Features

✅ **Table-Level Differential Analysis**
- Compares metadata (intent) vs UC (actual)
- Generates GRANTs for new privileges
- Generates REVOKEs for removed privileges

✅ **Idempotent Execution**
- Safe to run multiple times
- Converges to desired state
- No duplicates or errors

✅ **Non-Blocking**
- Logs errors but doesn't fail pipeline
- Continue on error by default
- Detailed error messages

✅ **Observable**
- Comprehensive stats logged
- Query from pipeline_stats table
- Track success rates over time

✅ **Flexible**
- Automatic (pipeline) or on-demand (standalone)
- Dry-run mode for testing
- Audit mode for compliance

## 📚 Additional Documentation

- [Full Implementation Design](../ACCESS_CONTROL_FINAL_IMPLEMENTATION.md)
- [OO Design Analysis](../ACCESS_CONTROL_OO_DESIGN_ANALYSIS.md)
- [Design Comparison](../ACCESS_CONTROL_DESIGN_COMPARISON.md)

## 🆘 Support

For issues or questions:
1. Check logs in pipeline execution
2. Review metadata files for errors
3. Verify AD groups are provisioned
4. Test with dry-run mode first
5. Use standalone tool for debugging