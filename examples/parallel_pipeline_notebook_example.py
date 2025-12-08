# Databricks notebook source
# MAGIC %md
# MAGIC # Parallel Pipeline Execution
# MAGIC
# MAGIC This notebook executes multiple pipelines in parallel using ThreadPoolExecutor.
# MAGIC Supports up to 16 concurrent pipeline executions.

# COMMAND ----------

from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime
from nova_framework.pipeline.orchestrator import Pipeline
import os

# COMMAND ----------

# MAGIC %md
# MAGIC ## Configuration
# MAGIC Set constants and volume path for auto-discovery

# COMMAND ----------

# Constants for all pipelines
SOURCE_REF = "2025-12-08"  # Date or source reference
ENV = "dev"  # Environment: dev, test, prod
PROCESS_QUEUE_ID_START = 8000  # Starting ID for process queue

# Volume path to discover pipeline folders
VOLUME_BASE_PATH = f"/Volumes/cluk_dev_nova/bronze_galahad/raw/{SOURCE_REF}"

# Maximum number of concurrent pipeline executions
MAX_WORKERS = 16

# COMMAND ----------

# MAGIC %md
# MAGIC ## Auto-Discover Pipelines from Volume Folders

# COMMAND ----------

def discover_pipelines(base_path, source_ref, env, start_id=8000):
    """Discover pipelines by reading folder names from volume path."""
    print(f"Discovering pipelines in: {base_path}")

    if not os.path.exists(base_path):
        raise ValueError(f"Volume path does not exist: {base_path}")

    # Get all directories
    folders = [f for f in os.listdir(base_path) if os.path.isdir(os.path.join(base_path, f))]

    if not folders:
        raise ValueError(f"No folders found in: {base_path}")

    folders.sort()
    print(f"Found {len(folders)} folders: {folders}")

    # Generate configs
    configs_list = []
    for idx, folder_name in enumerate(folders):
        configs_list.append({
            "process_queue_id": start_id + idx,
            "data_contract_name": f"data.galahad.{folder_name}",
            "source_ref": source_ref,
            "env": env
        })

    # Convert to dict
    return {cfg["data_contract_name"]: cfg for cfg in configs_list}

# Generate pipeline configs
pipeline_configs = discover_pipelines(VOLUME_BASE_PATH, SOURCE_REF, ENV, PROCESS_QUEUE_ID_START)
print(f"\nGenerated {len(pipeline_configs)} pipeline configurations")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Execution Functions

# COMMAND ----------

def execute_pipeline(pipeline_name, config):
    """Execute a single pipeline with timing and error handling."""
    start = datetime.now()
    print(f"[{pipeline_name}] Starting...")

    try:
        # If data_contract_name is the dict key, use it; otherwise get from config
        data_contract_name = config.get("data_contract_name", pipeline_name)

        pipeline = Pipeline()
        status = pipeline.run(
            process_queue_id=config["process_queue_id"],
            data_contract_name=data_contract_name,
            source_ref=config["source_ref"],
            env=config["env"]
        )

        duration = (datetime.now() - start).total_seconds()
        print(f"[{pipeline_name}] {'✅' if status == 'SUCCESS' else '❌'} {status} ({duration:.1f}s)")

        return {
            "name": pipeline_name,
            "status": status,
            "duration": duration,
            "error": None
        }

    except Exception as e:
        duration = (datetime.now() - start).total_seconds()
        print(f"[{pipeline_name}] ❌ ERROR: {str(e)}")

        return {
            "name": pipeline_name,
            "status": "FAILED",
            "duration": duration,
            "error": str(e)
        }

# COMMAND ----------

# MAGIC %md
# MAGIC ## Execute Pipelines in Parallel

# COMMAND ----------

print(f"Executing {len(pipeline_configs)} pipelines in parallel (max {MAX_WORKERS} concurrent)...\n")

results = []

with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
    futures = {
        executor.submit(execute_pipeline, name, config): name
        for name, config in pipeline_configs.items()
    }

    for future in as_completed(futures):
        results.append(future.result())

# COMMAND ----------

# MAGIC %md
# MAGIC ## Results Summary

# COMMAND ----------

# Print summary
succeeded = sum(1 for r in results if r["status"] == "SUCCESS")
failed = len(results) - succeeded
total_duration = sum(r["duration"] for r in results)

print("=" * 80)
print("EXECUTION SUMMARY")
print("=" * 80)
print(f"Total:     {len(results)}")
print(f"Succeeded: {succeeded}")
print(f"Failed:    {failed}")
print(f"Total Time: {total_duration:.1f}s")
print("=" * 80)

if failed > 0:
    print("\nFailed Pipelines:")
    for r in results:
        if r["status"] != "SUCCESS":
            print(f"  ❌ {r['name']}: {r['error']}")

# COMMAND ----------

# Display results as a table (optional)
display(spark.createDataFrame(results))
