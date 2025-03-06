# app/settings.py
import os
from pathlib import Path

# Base directory (two levels up from this file)
BASE_DIR = Path(__file__).resolve().parent.parent

# Directory constants
DATA_DIR = BASE_DIR / 'data'
# TEMPLATES_DIR = BASE_DIR / 'templates'
# STATIC_DIR = BASE_DIR / 'static'

TRANSFORMATION_DIR = f"transformation"
TRANSFORMATION_MASTERS_DIR = f"{TRANSFORMATION_DIR}/masters"
TRANSFORMATION_LOGS_DIR = f"{BASE_DIR}/data/transformation/logs"
TRANSFORMATION_ZIP_DIR = f"{BASE_DIR}/data/transformation/zipFiles"
TRANSFORMATION_DEFAULTFILES_DIR = f"{BASE_DIR}/data/transformation/defaultFiles"
TRANSFORMATION_OUTPUT_DIR = f"{BASE_DIR}/data/transformation/outputs"
TRANSFORMATION_DUMPFILES_DIR = f"{BASE_DIR}/data/transformation/dumpFiles"
TRANSFORMATION_TEMPLATEFILES_DIR = f"{BASE_DIR}/data/transformation/templateFiles"

BIN_DIR = f"{BASE_DIR}/bin"
LIB_DIR = f"{BASE_DIR}/libs"
JAR_DIR = f"{LIB_DIR}/jars"

'''
fs.azure.account.key.r2rdevblobstorage.dfs.core.windows.net = DHV2FFz74wfz4oPqXYb8Ll4McBtxzZGLLwMxs+Jkes7kOP8k/gTF8NY73zOZ5CBSN/FLxXM30w4M+AStMK/tZw==
spark.sql.warehouse.dir  = abfss://iceberg-storage@r2rdevblobstorage.dfs.core.windows.net
'''
