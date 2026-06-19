#!/usr/bin/env python3
"""
Example: Basic Usage of tse_tick Package

This script demonstrates the most common use cases for the tse_tick package.
"""

import tse_tick
import pandas as pd
from pathlib import Path


def main():
    """Main function demonstrating basic usage"""

    print("=" * 70)
    print("tse_tick Basic Usage Example")
    print("=" * 70)

    # Example 1: Load individual stock data
    print("\n1. Loading Individual Stock Data (TICST120)")
    print("-" * 70)

    stock_file = "path/to/HTICST120.20230104.1.zip"

    # Check if file exists (replace with your actual path)
    if not Path(stock_file).exists():
        print(f"⚠️  File not found: {stock_file}")
        print("Please update the path in the script.")
        stock_file = None

    if stock_file:
        # Load with English columns
        df = tse_tick.create_df(stock_file, language='en', rows=1000)

        print(f"✓ Loaded {len(df)} rows")
        print(f"✓ Columns: {len(df.columns)}")
        print(f"\nFirst 5 rows:")
        print(df.head())

        # Export to CSV
        output_file = tse_tick.export_to_csv(
            stock_file,
            output_path="stock_data_sample.csv",
            language='en',
            rows=1000
        )
        print(f"\n✓ Exported to: {output_file}")

    # Example 2: Load with Japanese column names
    print("\n\n2. Loading with Japanese Column Names")
    print("-" * 70)

    if stock_file:
        df_jp = tse_tick.create_df(stock_file, language='jp', rows=100)

        print(f"✓ Loaded {len(df_jp)} rows with Japanese columns")
        print(f"\nJapanese column names (first 10):")
        for i, col in enumerate(df_jp.columns[:10], 1):
            print(f"{i:2}. {col}")

    # Example 3: Different data types
    print("\n\n3. Working with Different Data Types")
    print("-" * 70)

    data_examples = {
        "Stock Summary": "path/to/HTICSS110.202301.zip",
        "Index Tick": "path/to/HTICIT110.202301.zip",
        "Index Summary": "path/to/HTICIS110.202301.zip",
    }

    for name, path in data_examples.items():
        if Path(path).exists():
            df_temp = tse_tick.create_df(path, language='en', rows=10)
            print(f"✓ {name:15} - Shape: {df_temp.shape}")
        else:
            print(f"⚠️  {name:15} - File not found: {path}")

    # Example 4: Accessing schemas
    print("\n\n4. Accessing Schema Information")
    print("-" * 70)

    # Get schema for individual stock data
    schema = tse_tick.get_schema_individual_stock_95()
    print(f"✓ Individual stock schema has {len(schema)} fields")
    print(f"\nFirst 10 fields:")
    for i, field in enumerate(schema[:10], 1):
        print(f"{i:2}. {field}")

    # Get Japanese-English mapping
    mapping = tse_tick.get_japanese_column_mapping()
    print(f"\n✓ Column mapping dictionary has {len(mapping)} entries")

    # Example 5: Package information
    print("\n\n5. Package Information")
    print("-" * 70)
    print(tse_tick.get_info())

    print("\n" + "=" * 70)
    print("Example completed!")
    print("=" * 70)


if __name__ == "__main__":
    main()
