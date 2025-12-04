# Databricks notebook source
"""
Pipeline Workflow Notebook

Entry point for Databricks workflows to execute pipelines.
Parameters are passed via Databricks widgets.
"""

# COMMAND ----------

# Get parameters from Databricks widgets
dbutils.widgets.text("process_queue_id", "", "Process Queue ID")
dbutils.widgets.text("data_contract_name", "", "Data Contract Name")
dbutils.widgets.text("source_ref", "", "Source Reference")
dbutils.widgets.text("env", "", "Environment")

process_queue_id = int(dbutils.widgets.get("process_queue_id"))
data_contract_name = dbutils.widgets.get("data_contract_name")
source_ref = dbutils.widgets.get("source_ref")
env = dbutils.widgets.get("env")

print(f"[PipelineFlow] Process Queue ID: {process_queue_id}")
print(f"[PipelineFlow] Data Contract: {data_contract_name}")
print(f"[PipelineFlow] Source Reference: {source_ref}")
print(f"[PipelineFlow] Environment: {env}")

# COMMAND ----------

# Import and run pipeline
from nova_framework.pipeline.orchestrator import Pipeline

print("[PipelineFlow] Executing pipeline...")

pipeline = Pipeline()
result = pipeline.run(
    process_queue_id=process_queue_id,
    data_contract_name=data_contract_name,
    source_ref=source_ref,
    env=env
)

print(f"[PipelineFlow] Pipeline execution completed: {result}")

# COMMAND ----------

# Validate result
if result != "SUCCESS":
    raise RuntimeError(f"Pipeline failed with result: {result}")

print("[PipelineFlow] ✅ Pipeline completed successfully")
