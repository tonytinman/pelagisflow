# Parallel Pipeline Execution Guide

This guide shows how to execute multiple pipelines in parallel in Databricks.

## Quick Start

There are two options:

### Option 1: Simple Python Script (Copy & Paste)

Use `parallel_pipeline_runner.py` - a standalone script you can copy into any notebook:

```python
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime
from nova_framework.pipeline.orchestrator import Pipeline

# Define pipeline configurations
pipeline_configs = {
    "pipeline_1": {
        "process_queue_id": 175,
        "data_contract_name": "data.galahad.quolive_manager_staff_admin",
        "source_ref": "2025-12-08",
        "env": "dev"
    },
    "pipeline_2": {
        "process_queue_id": 176,
        "data_contract_name": "data.galahad.customers",
        "source_ref": "2025-12-08",
        "env": "dev"
    },
    # Add more pipelines...
}

# Run in parallel (up to 16 concurrent)
results = {}
with ThreadPoolExecutor(max_workers=16) as executor:
    futures = {
        executor.submit(
            lambda name, cfg: Pipeline().run(
                cfg["process_queue_id"],
                cfg["data_contract_name"],
                cfg["source_ref"],
                cfg["env"]
            ),
            name,
            config
        ): name
        for name, config in pipeline_configs.items()
    }

    for future in as_completed(futures):
        name = futures[future]
        results[name] = future.result()

# Check results
for name, status in results.items():
    print(f"{name}: {status}")
```

### Option 2: Full Databricks Notebook

Import `parallel_pipeline_notebook_example.py` as a Databricks notebook for a complete solution with:
- Progress tracking
- Error handling
- Results summary table
- Timing statistics

## Configuration

Adjust the `pipeline_configs` dictionary with your pipeline parameters:

```python
pipeline_configs = {
    "unique_pipeline_name": {
        "process_queue_id": 175,           # Your process queue ID
        "data_contract_name": "...",       # Full contract name
        "source_ref": "2025-12-08",        # Date or reference
        "env": "dev"                       # Environment: dev/test/prod
    }
}
```

## Concurrency

- Default: 16 concurrent pipelines (adjustable via `MAX_WORKERS`)
- Recommended: Match your Databricks cluster capacity
- Smaller clusters: Use 4-8 workers
- Larger clusters: Use 12-16 workers

## Best Practices

1. **Group similar pipelines** - Run pipelines with similar data sources together
2. **Monitor resources** - Watch cluster CPU/memory during execution
3. **Start small** - Test with 2-3 pipelines before scaling to 16
4. **Use unique queue IDs** - Each pipeline should have a unique `process_queue_id`
5. **Handle failures gracefully** - Check results and re-run failed pipelines if needed

## Troubleshooting

**Out of memory errors:**
- Reduce `MAX_WORKERS` to 4-8
- Increase cluster size
- Run pipelines in smaller batches

**Pipeline failures:**
- Check individual pipeline logs
- Verify data contract names are correct
- Ensure process queue IDs are unique
- Validate source references exist

**Slow execution:**
- Ensure pipelines can run independently
- Check for shared resource contention
- Verify cluster has sufficient cores for parallel execution
