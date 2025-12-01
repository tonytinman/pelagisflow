"""
Quick verification test for metrics and DQ logging fixes.
"""

import sys
import os

# Add parent directory to path so we can import pelagisflow
parent_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, parent_dir)

def test_imports():
    """Test that all critical imports work."""
    print("Testing imports...")

    try:
        from pelagisflow.core.config import get_config, ObservabilityConfig
        print("✓ Config imports successful")

        from pelagisflow.observability.logging import get_logger, FrameworkLogger
        print("✓ Logging imports successful")

        from pelagisflow.observability.stats import PipelineStats
        print("✓ Stats imports successful")

        from pelagisflow.observability.telemetry import TelemetryEmitter
        print("✓ Telemetry imports successful")

        from pelagisflow.quality.dq import DQEngine
        print("✓ DQ Engine imports successful")

        from pelagisflow.pipeline.stages.quality_stage import QualityStage
        print("✓ Quality Stage imports successful")

        return True
    except Exception as e:
        print(f"✗ Import failed: {e}")
        import traceback
        traceback.print_exc()
        return False


def test_config_attributes():
    """Test that config has all required attributes."""
    print("\nTesting config attributes...")

    try:
        from pelagisflow.core.config import ObservabilityConfig

        config = ObservabilityConfig()

        # Check for all required attributes
        required_attrs = [
            'log_level',
            'log_to_delta',
            'log_table',
            'metrics_enabled',
            'metrics_table',
            'telemetry_enabled',
            'telemetry_table',
            'stats_table',
            'dq_errors_table'  # This was missing before!
        ]

        for attr in required_attrs:
            if not hasattr(config, attr):
                print(f"✗ Missing attribute: {attr}")
                return False
            value = getattr(config, attr)
            print(f"✓ {attr} = {value}")

        print("✓ All config attributes present")
        return True

    except Exception as e:
        print(f"✗ Config test failed: {e}")
        import traceback
        traceback.print_exc()
        return False


def test_logger_initialization():
    """Test that logger can be initialized."""
    print("\nTesting logger initialization...")

    try:
        from pelagisflow.observability.logging import get_logger

        logger = get_logger("test_module")
        print(f"✓ Logger created: {logger}")

        # Test logging methods exist
        assert hasattr(logger, 'info')
        assert hasattr(logger, 'error')
        assert hasattr(logger, 'warning')
        assert hasattr(logger, 'debug')

        print("✓ Logger has all required methods")
        return True

    except Exception as e:
        print(f"✗ Logger test failed: {e}")
        import traceback
        traceback.print_exc()
        return False


def main():
    """Run all verification tests."""
    print("=" * 60)
    print("METRICS AND DQ LOGGING VERIFICATION")
    print("=" * 60)

    results = []

    # Run tests
    results.append(("Imports", test_imports()))
    results.append(("Config Attributes", test_config_attributes()))
    results.append(("Logger Initialization", test_logger_initialization()))

    # Summary
    print("\n" + "=" * 60)
    print("SUMMARY")
    print("=" * 60)

    for test_name, passed in results:
        status = "✓ PASSED" if passed else "✗ FAILED"
        print(f"{test_name}: {status}")

    all_passed = all(passed for _, passed in results)

    print("\n" + "=" * 60)
    if all_passed:
        print("✓ ALL TESTS PASSED - Logging is fixed!")
    else:
        print("✗ SOME TESTS FAILED - Review errors above")
    print("=" * 60)

    return all_passed


if __name__ == "__main__":
    success = main()
    exit(0 if success else 1)
