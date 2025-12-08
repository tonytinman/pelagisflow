"""
Standalone Parallel Pipeline Runner for Databricks Notebooks

Copy this entire script into a Databricks notebook cell and run it.
Adjust the pipeline_configs dictionary with your pipeline parameters.

Usage:
    1. Copy this script into a notebook cell
    2. Update pipeline_configs with your pipeline details
    3. Run the cell
"""

from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime
from nova_framework.pipeline.orchestrator import Pipeline

# ============================================================================
# CONFIGURATION - Update this section with your pipeline configurations
# ============================================================================

pipeline_configs = {
    "pipeline_1": {
        "process_queue_id": 175,
        "data_contract_name": "data.galahad.quolive_manager_staff_admin",
        "source_ref": "2025-12-08",
        "env": "dev"
    },
    "pipeline_2": {
        "process_queue_id": 176,
        "data_contract_name": "data.galahad.another_contract",
        "source_ref": "2025-12-08",
        "env": "dev"
    },
    # Add more pipelines as needed (up to 16 will run in parallel)
}

# Maximum number of pipelines to run concurrently
MAX_WORKERS = 16

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
        pipeline = Pipeline()
        execution_result = pipeline.run(
            process_queue_id=config["process_queue_id"],
            data_contract_name=config["data_contract_name"],
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
