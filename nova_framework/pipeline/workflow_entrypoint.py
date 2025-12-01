"""
Nova Framework Pipeline Entry Point

This script serves as the main entry point for executing pipelines from Databricks workflows.

Usage:
    python workflow_entrypoint.py \\
        --process_queue_id 1 \\
        --data_contract_name customer_data \\
        --source_ref 2024-11-28 \\
        --env dev
"""

import os
import sys

# Add the repository root to the system path for importing nova_framework
# Go up two directories from this file's location: pipeline/ -> nova_framework/ -> repo_root/
repo_root = os.path.abspath(os.path.join(os.path.dirname(__file__), "../.."))
sys.path.insert(0, repo_root)

# Import Pipeline from nova_framework
# This will work with either import style:
# - from nova_framework.pipeline.orchestrator import Pipeline (direct import)
# - from nova_framework.pipeline import Pipeline (after __init__.py is in place)
try:
    # Preferred: Clean module-level import (requires pipeline/__init__.py)
    from nova_framework.pipeline import Pipeline
except ImportError:
    # Fallback: Direct import (works without __init__.py)
    from nova_framework.pipeline.orchestrator import Pipeline


def main():
    """
    Parses workflow parameters and calls Pipeline.run().
    
    Expects arguments in the following form:
        --process_queue_id <int>
        --data_contract_name <string>
        --source_ref <string>
        --env <string>
    """

    print("[Nova Framework] Workflow has started running")

    args = sys.argv[1:]
    if len(args) < 8:
        raise ValueError(
            f"Expected parameters: --process_queue_id <int> "
            f"--data_contract_name <string> --source_ref <string> --env <string>. Got: {args}"
        )

    # Convert CLI args into key/value pairs
    arg_map = dict(zip(args[::2], args[1::2]))

    # Parse and validate parameters
    process_queue_id_str = arg_map.get("--process_queue_id")
    if process_queue_id_str is None:
        raise ValueError(f"Missing required parameter --process_queue_id. Got: {args}")
    process_queue_id = int(process_queue_id_str)

    data_contract_name = arg_map.get("--data_contract_name")
    if data_contract_name is None:
        raise ValueError(f"Missing required parameter --data_contract_name. Got: {args}")

    source_ref = arg_map.get("--source_ref")
    if source_ref is None:
        raise ValueError(f"Missing required parameter --source_ref. Got: {args}")

    env = arg_map.get("--env")
    if env is None:
        raise ValueError(f"Missing required parameter --env. Got: {args}")

    # Log parameters
    print(f"[Nova Framework] Process Queue ID: {process_queue_id}")
    print(f"[Nova Framework] Data Contract Name: {data_contract_name}")
    print(f"[Nova Framework] Source Reference: {source_ref}")
    print(f"[Nova Framework] Environment: {env}")

    try:
        # Create pipeline orchestrator
        pipeline = Pipeline()
        
        # Execute pipeline
        result = pipeline.run(
            process_queue_id=process_queue_id,
            data_contract_name=data_contract_name,
            source_ref=source_ref,
            env=env
        )
        
        print(f"[Nova Framework] Pipeline execution completed with result: {result}")
        
        # Exit with appropriate code
        if result == "SUCCESS":
            print("[Nova Framework] ✅ Pipeline completed successfully")
            sys.exit(0)
        else:
            print(f"[Nova Framework] ❌ Pipeline failed with result: {result}")
            sys.exit(1)
            
    except Exception as e:
        print(f"[Nova Framework] ❌ Pipeline execution failed: {e}")
        import traceback
        traceback.print_exc()
        raise


if __name__ == "__main__":
    main()