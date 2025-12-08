"""
Standalone Parallel Pipeline Runner for Databricks Notebooks

Auto-discovers pipelines by reading folder names from a Databricks volume path.
Generates pipeline configs with auto-incrementing process_queue_ids.

Usage:
    1. Copy this script into a notebook cell
    2. Update the constants (SOURCE_REF, ENV, VOLUME_BASE_PATH)
    3. Run the cell - it will discover and execute all pipelines in parallel

Example:
    If your volume path contains folders: customers, orders, products
    It will automatically execute:
    - data.galahad.customers (queue_id: 8000)
    - data.galahad.orders (queue_id: 8001)
    - data.galahad.products (queue_id: 8002)
"""

from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime
from nova_framework.pipeline.orchestrator import Pipeline
import os

# ============================================================================
# CONFIGURATION - Update these constants
# ============================================================================

# Constants for all pipelines
SOURCE_REF = "2025-12-08"  # Date or source reference
ENV = "dev"  # Environment: dev, test, prod
PROCESS_QUEUE_ID_START = 8000  # Starting ID for process queue

# Volume path to discover pipeline folders
VOLUME_BASE_PATH = f"/Volumes/cluk_dev_nova/bronze_galahad/raw/{SOURCE_REF}"

# Maximum number of pipelines to run concurrently
MAX_WORKERS = 16

# ============================================================================
# AUTO-DISCOVER PIPELINES FROM VOLUME FOLDERS
# ============================================================================

def discover_pipelines(base_path, source_ref, env, start_id=8000):
    """
    Discover pipelines by reading folder names from volume path.

    Returns list of pipeline configs with auto-generated IDs.
    """
    print(f"Discovering pipelines in: {base_path}")

    if not os.path.exists(base_path):
        raise ValueError(f"Volume path does not exist: {base_path}")

    # Get all directories in the volume path
    folders = [
        f for f in os.listdir(base_path)
        if os.path.isdir(os.path.join(base_path, f))
    ]

    if not folders:
        raise ValueError(f"No folders found in: {base_path}")

    folders.sort()  # Sort for consistent ordering
    print(f"Found {len(folders)} folders: {folders}")

    # Generate pipeline configs
    configs_list = []
    for idx, folder_name in enumerate(folders):
        config = {
            "process_queue_id": start_id + idx,
            "data_contract_name": f"data.galahad.{folder_name}",
            "source_ref": source_ref,
            "env": env
        }
        configs_list.append(config)

    # Convert to dict with data_contract_name as key
    pipeline_configs = {cfg["data_contract_name"]: cfg for cfg in configs_list}

    print(f"Generated {len(pipeline_configs)} pipeline configurations")
    return pipeline_configs


# Generate pipeline configs from volume folders
pipeline_configs = discover_pipelines(
    base_path=VOLUME_BASE_PATH,
    source_ref=SOURCE_REF,
    env=ENV,
    start_id=PROCESS_QUEUE_ID_START
)

# ============================================================================
# EXECUTION LOGIC - No need to modify below this line
# ============================================================================

def execute_single_pipeline(pipeline_name, config):
    """Execute a single pipeline and return results."""
    start_time = datetime.now()
    print(f"[{pipeline_name}] Starting at {start_time.strftime('%H:%M:%S')}")

    result = {
        "pipeline_name": pipeline_name,
        "status": "FAILED",
        "start_time": start_time.isoformat(),
        "config": config
    }

    try:
        # If data_contract_name is the dict key, add it to config
        data_contract_name = config.get("data_contract_name", pipeline_name)

        pipeline = Pipeline()
        execution_result = pipeline.run(
            process_queue_id=config["process_queue_id"],
            data_contract_name=data_contract_name,
            source_ref=config["source_ref"],
            env=config["env"]
        )

        result["status"] = execution_result

    except Exception as e:
        result["status"] = "FAILED"
        result["error"] = str(e)
        print(f"[{pipeline_name}] ERROR: {str(e)}")

    finally:
        end_time = datetime.now()
        duration = (end_time - start_time).total_seconds()
        result["end_time"] = end_time.isoformat()
        result["duration_seconds"] = duration

        status_symbol = "✅" if result["status"] == "SUCCESS" else "❌"
        print(f"[{pipeline_name}] {status_symbol} Completed in {duration:.2f}s - Status: {result['status']}")

    return result


def run_pipelines_in_parallel(configs, max_workers=16):
    """Execute multiple pipelines in parallel."""
    print(f"\n{'='*80}")
    print(f"Starting parallel execution of {len(configs)} pipelines")
    print(f"Max concurrent workers: {max_workers}")
    print(f"{'='*80}\n")

    results = {}

    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        future_to_pipeline = {
            executor.submit(execute_single_pipeline, name, config): name
            for name, config in configs.items()
        }

        for future in as_completed(future_to_pipeline):
            pipeline_name = future_to_pipeline[future]
            try:
                result = future.result()
                results[pipeline_name] = result
            except Exception as e:
                print(f"[{pipeline_name}] Executor error: {str(e)}")
                results[pipeline_name] = {
                    "pipeline_name": pipeline_name,
                    "status": "FAILED",
                    "error": str(e)
                }

    return results


def print_summary(results):
    """Print execution summary."""
    total = len(results)
    succeeded = sum(1 for r in results.values() if r["status"] == "SUCCESS")
    failed = total - succeeded

    print(f"\n{'='*80}")
    print("EXECUTION SUMMARY")
    print(f"{'='*80}")
    print(f"Total Pipelines:  {total}")
    print(f"Succeeded:        {succeeded}")
    print(f"Failed:           {failed}")
    print(f"{'='*80}\n")

    if failed > 0:
        print("Failed Pipelines:")
        for name, result in results.items():
            if result["status"] != "SUCCESS":
                error = result.get("error", "Unknown error")
                print(f"  ❌ {name}: {error}")
        print()


# ============================================================================
# EXECUTE
# ============================================================================

if __name__ == "__main__":
    results = run_pipelines_in_parallel(pipeline_configs, max_workers=MAX_WORKERS)
    print_summary(results)
