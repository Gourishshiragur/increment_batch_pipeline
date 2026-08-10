# Databricks notebook source
"""
00_environment_setup.py

Creates the required Unity Catalog Volume folder structure
for the Incremental Batch Pipeline.
"""

from utils.config_loader import (
    get_environment,
    get_base_path,
    get_paths,
)

# Ensure this is only executed in Databricks
if get_environment() != "databricks":
    raise RuntimeError("Environment setup should only be executed in Databricks.")

base_path = get_base_path()
folders = get_paths()["folders"]

print(f"Base Storage Path : {base_path}")
print("-" * 60)

for name, folder in folders.items():
    path = f"{base_path}/{folder}"

    try:
        dbutils.fs.mkdirs(path)
        print(f"✓ Created: {name:<20} -> {path}")

    except Exception as e:
        print(f"✗ Failed : {name:<20} -> {path}")
        raise e

print("-" * 60)
print("Databricks environment setup completed successfully.")