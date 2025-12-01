"""
Quick test to verify transformation module imports work correctly.
"""

import sys
import os

# Add current directory to path so we can import nova_framework
current_dir = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, current_dir)

def test_transformation_imports():
    """Test that transformation module imports work."""
    print("Testing transformation imports...")

    try:
        # Test transformation module
        from nova_framework.transformation import TransformationStrategy, TransformationType
        print("✓ TransformationStrategy imported successfully")

        from nova_framework.transformation import TransformationRegistry, TransformationMetadata
        print("✓ TransformationRegistry imported successfully")

        from nova_framework.transformation import TransformationLoader
        print("✓ TransformationLoader imported successfully")

        # Test transformation stage
        from nova_framework.pipeline.stages.transformation_stage import TransformationStage
        print("✓ TransformationStage imported successfully")

        return True
    except Exception as e:
        print(f"✗ Import failed: {e}")
        import traceback
        traceback.print_exc()
        return False


def test_core_imports():
    """Test that core imports still work."""
    print("\nTesting core imports...")

    try:
        from nova_framework.core.config import get_config, ObservabilityConfig
        print("✓ Config imports successful")

        config = ObservabilityConfig()
        if hasattr(config, 'dq_errors_table'):
            print(f"✓ dq_errors_table config present: {config.dq_errors_table}")
        else:
            print("✗ dq_errors_table config missing!")
            return False

        return True
    except Exception as e:
        print(f"✗ Core import failed: {e}")
        import traceback
        traceback.print_exc()
        return False


def main():
    """Run all tests."""
    print("=" * 60)
    print("TRANSFORMATION MODULE IMPORT VERIFICATION")
    print("=" * 60)

    results = []
    results.append(("Transformation Imports", test_transformation_imports()))
    results.append(("Core Imports", test_core_imports()))

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
        print("✓ ALL TESTS PASSED - Transformation modules ready!")
    else:
        print("✗ SOME TESTS FAILED - Review errors above")
    print("=" * 60)

    return all_passed


if __name__ == "__main__":
    success = main()
    exit(0 if success else 1)
