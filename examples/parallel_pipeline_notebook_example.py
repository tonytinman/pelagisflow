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

# COMMAND ----------

# MAGIC %md
# MAGIC ## Configuration
# MAGIC Define your pipeline configurations below

# COMMAND ----------

# Define your pipeline configurations
pipeline_configs = {
    "staff_admin_pipeline": {
        "process_queue_id": 175,
        "data_contract_name": "data.galahad.quolive_manager_staff_admin",
        "source_ref": "2025-12-08",
        "env": "dev"
    },
    "customer_pipeline": {
        "process_queue_id": 176,
        "data_contract_name": "data.galahad.customers",
        "source_ref": "2025-12-08",
        "env": "dev"
    },
    "orders_pipeline": {
        "process_queue_id": 177,
        "data_contract_name": "data.galahad.orders",
        "source_ref": "2025-12-08",
        "env": "dev"
    },
    # Add up to 16 pipelines total
}

# Maximum number of concurrent pipeline executions
MAX_WORKERS = 16

# COMMAND ----------

# MAGIC %md
# MAGIC ## Execution Functions

# COMMAND ----------

def execute_pipeline(pipeline_name, config):
    """Execute a single pipeline with timing and error handling."""
    start = datetime.now()
    print(f"[{pipeline_name}] Starting...")

    try:
        pipeline = Pipeline()
        status = pipeline.run(
            process_queue_id=config["process_queue_id"],
            data_contract_name=config["data_contract_name"],
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
