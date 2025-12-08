# Parallel Pipeline Execution Guide

This guide shows how to execute multiple pipelines in parallel in Databricks.

## Quick Start

There are two options:

### Option 1: Simple Python Script (Copy & Paste)

Use `parallel_pipeline_runner.py` - a standalone script you can copy into any notebook:

```python
from concurrent.futures import ThreadPoolExecutor, as_completed
from nova_framework.pipeline.orchestrator import Pipeline
import os

# Set your constants
SOURCE_REF = "2025-12-08"
ENV = "dev"
PROCESS_QUEUE_ID_START = 8000
VOLUME_BASE_PATH = f"/Volumes/cluk_dev_nova/bronze_galahad/raw/{SOURCE_REF}"

# Auto-discover pipelines from volume folders
folders = [f for f in os.listdir(VOLUME_BASE_PATH) if os.path.isdir(os.path.join(VOLUME_BASE_PATH, f))]
folders.sort()

configs = [
    {
        "process_queue_id": PROCESS_QUEUE_ID_START + idx,
        "data_contract_name": f"data.galahad.{folder}",
        "source_ref": SOURCE_REF,
        "env": ENV
    }
    for idx, folder in enumerate(folders)
]

pipeline_configs = {cfg["data_contract_name"]: cfg for cfg in configs}
print(f"Discovered {len(pipeline_configs)} pipelines")

# Run in parallel
def run_pipeline(contract_name, cfg):
    pipeline = Pipeline()
    return pipeline.run(
        cfg["process_queue_id"],
        cfg.get("data_contract_name", contract_name),
        cfg["source_ref"],
        cfg["env"]
    )

results = {}
with ThreadPoolExecutor(max_workers=16) as executor:
    futures = {executor.submit(run_pipeline, name, config): name for name, config in pipeline_configs.items()}
    for future in as_completed(futures):
        name = futures[future]
        results[name] = future.result()
        print(f"{name}: {results[name]}")
```

### Option 2: Full Databricks Notebook

Import `parallel_pipeline_notebook_example.py` as a Databricks notebook for a complete solution with:
- Progress tracking
- Error handling
- Results summary table
- Timing statistics

## Configuration

The script auto-discovers pipelines by reading folder names from a volume path. Just set these constants:

```python
# Constants
SOURCE_REF = "2025-12-08"  # Your date or source reference
ENV = "dev"  # Environment: dev/test/prod
PROCESS_QUEUE_ID_START = 8000  # Starting queue ID (auto-increments)

# Volume path - script will discover all folders here
VOLUME_BASE_PATH = f"/Volumes/cluk_dev_nova/bronze_galahad/raw/{SOURCE_REF}"
```

The script will:
1. List all folders in the volume path
2. Generate `data.galahad.{folder_name}` for each folder
3. Auto-assign process_queue_id starting from 8000 (8000, 8001, 8002, ...)
4. Create pipeline configs with your SOURCE_REF and ENV

Example: If the volume contains folders `customers`, `orders`, `products`, it generates:
```python
{
    "data.galahad.customers": {"process_queue_id": 8000, "source_ref": "2025-12-08", "env": "dev"},
    "data.galahad.orders": {"process_queue_id": 8001, "source_ref": "2025-12-08", "env": "dev"},
    "data.galahad.products": {"process_queue_id": 8002, "source_ref": "2025-12-08", "env": "dev"}
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
