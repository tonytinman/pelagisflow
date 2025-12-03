"""
Pipeline Workflow Entry Point

This script provides the entry point for Databricks workflows to execute pipelines.
It handles CLI argument parsing and delegates to the Pipeline orchestrator.

Usage:
    python workflow_entrypoint.py \
        --process_queue_id 1 \
        --data_contract_name customer_data \
        --source_ref 2024-11-28 \
        --env dev
"""

import os
import sys

# Add project root to Python path to enable imports
# This allows imports like: from pipeline.orchestrator import Pipeline
project_root = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
if project_root not in sys.path:
    sys.path.insert(0, project_root)

# Import Pipeline from orchestrator
from pipeline.orchestrator import Pipeline


def main():
    """
    Parse workflow parameters and execute pipeline.

    Expects arguments in the form:
        --process_queue_id <int>
        --data_contract_name <string>
        --source_ref <string>
        --env <string>
    """

    print("[PipelineFlow] Workflow entry point started")

    # Parse CLI arguments
    args = sys.argv[1:]
    if len(args) < 8:
        raise ValueError(
            f"Expected 8 arguments: --process_queue_id <int> "
            f"--data_contract_name <string> --source_ref <string> --env <string>. "
            f"Got {len(args)}: {args}"
        )

    # Convert CLI args into key/value pairs
    arg_map = dict(zip(args[::2], args[1::2]))

    # Parse and validate required parameters
    process_queue_id_str = arg_map.get("--process_queue_id")
    if process_queue_id_str is None:
        raise ValueError(f"Missing required parameter: --process_queue_id")
    process_queue_id = int(process_queue_id_str)

    data_contract_name = arg_map.get("--data_contract_name")
    if data_contract_name is None:
        raise ValueError(f"Missing required parameter: --data_contract_name")

    source_ref = arg_map.get("--source_ref")
    if source_ref is None:
        raise ValueError(f"Missing required parameter: --source_ref")

    env = arg_map.get("--env")
    if env is None:
        raise ValueError(f"Missing required parameter: --env")

    # Log parameters
    print(f"[PipelineFlow] Process Queue ID: {process_queue_id}")
    print(f"[PipelineFlow] Data Contract: {data_contract_name}")
    print(f"[PipelineFlow] Source Reference: {source_ref}")
    print(f"[PipelineFlow] Environment: {env}")

    try:
        # Create Pipeline orchestrator instance
        pipeline = Pipeline()

        # Execute pipeline
        print(f"[PipelineFlow] Executing pipeline...")
        result = pipeline.run(
            process_queue_id=process_queue_id,
            data_contract_name=data_contract_name,
            source_ref=source_ref,
            env=env
        )

        # Handle result
        print(f"[PipelineFlow] Pipeline execution completed: {result}")

        if result == "SUCCESS":
            print("[PipelineFlow] ✅ Pipeline completed successfully")
            sys.exit(0)
        else:
            print(f"[PipelineFlow] ❌ Pipeline failed with result: {result}")
            sys.exit(1)

    except Exception as e:
        print(f"[PipelineFlow] ❌ Pipeline execution failed with exception: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)


if __name__ == "__main__":
    main()
