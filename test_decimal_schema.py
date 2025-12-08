"""
Test script to verify decimal type parsing in DataContract

Run this in a Databricks notebook to check if your contract is correctly
generating DecimalType for decimal columns.
"""

from nova_framework.contract.contract import DataContract

# Replace with your actual contract name
CONTRACT_NAME = "data.galahad.gallive_manager_life_claim"
ENV = "dev"

print("="*80)
print("DECIMAL TYPE VERIFICATION TEST")
print("="*80)

# Load contract
contract = DataContract(CONTRACT_NAME, ENV)

# Print full spark schema
print("\nFull Spark Schema:")
contract.spark_schema.printSchema()

# Find columns with "decimal" in their type definition
print("\nDecimal Columns Analysis:")
print("-"*80)

for col in contract.columns:
    col_name = col["name"]
    col_type = col.get("type", "string")

    # Only show columns that have "decimal" in the type
    if "decimal" in col_type.lower():
        # Get the actual Spark type that was generated
        spark_field = [f for f in contract.spark_schema.fields if f.name == col_name][0]
        spark_type = spark_field.dataType

        print(f"\nColumn: {col_name}")
        print(f"  Contract Type: {col_type}")
        print(f"  Spark Type: {spark_type}")
        print(f"  Type Class: {type(spark_type).__name__}")

        # Check if it's correctly DecimalType
        from pyspark.sql.types import DecimalType, StringType
        if isinstance(spark_type, DecimalType):
            print(f"  ✅ Correct! DecimalType(precision={spark_type.precision}, scale={spark_type.scale})")
        elif isinstance(spark_type, StringType):
            print(f"  ❌ ERROR! Should be DecimalType but got StringType")
        else:
            print(f"  ⚠️  Unexpected type: {type(spark_type)}")

print("\n" + "="*80)
print("TEST COMPLETE")
print("="*80)
