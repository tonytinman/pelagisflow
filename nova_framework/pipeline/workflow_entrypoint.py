"""
Pipeline Workflow Entry Point

Entry point for Databricks workflows to execute pipelines.

Usage:
    python workflow_entrypoint.py \
        --process_queue_id 1 \
        --data_contract_name customer_data \
        --source_ref 2024-11-28 \
        --env dev
"""

import sys
from nova_framework.pipeline.orchestrator import Pipeline


def main():
    """Parse parameters and execute pipeline."""

    print("[PipelineFlow] Workflow started")

    # Parse CLI arguments into key/value pairs
    args = sys.argv[1:]
    arg_map = dict(zip(args[::2], args[1::2]))

    # Extract required parameters
    process_queue_id = int(arg_map.get("--process_queue_id"))
    data_contract_name = arg_map.get("--data_contract_name")
    source_ref = arg_map.get("--source_ref")
    env = arg_map.get("--env")

    # Log parameters
    print(f"[PipelineFlow] Process Queue ID: {process_queue_id}")
    print(f"[PipelineFlow] Data Contract: {data_contract_name}")
    print(f"[PipelineFlow] Source Reference: {source_ref}")
    print(f"[PipelineFlow] Environment: {env}")

    # Execute pipeline
    print("[PipelineFlow] Executing pipeline...")
    pipeline = Pipeline()
    result = pipeline.run(
        process_queue_id=process_queue_id,
        data_contract_name=data_contract_name,
        source_ref=source_ref,
        env=env
    )

    # Handle result
    print(f"[PipelineFlow] Pipeline execution completed: {result}")

    if result != "SUCCESS":
        raise RuntimeError(f"Pipeline failed with result: {result}")

    print("[PipelineFlow] ✅ Pipeline completed successfully")


if __name__ == "__main__":
    main()
