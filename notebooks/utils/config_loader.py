# Databricks notebook source

# COMMAND ----------
"""
Configuration loader for FreshSip Beverages CPG Data Platform.

Reads pipeline configuration from a YAML file (default: config/pipeline_config.yaml)
and provides typed accessor helpers used by all pipeline modules.

Usage:
    from src.utils.config_loader import load_config, get_table_config, get_source_path

    config = load_config()
    tbl = config["tables"]["bronze"]["pos_transactions"]
    path = get_source_path(config, "pos", "pos_transactions")
"""

import os
from pathlib import Path
from typing import Any, Optional

import yaml


# COMMAND ----------
# Default config file path — relative to project root
_DEFAULT_CONFIG_PATH = Path(__file__).parent.parent.parent / "config" / "pipeline_config.yaml"


def load_config(config_path: Optional[str] = None) -> dict:
    """
    Load pipeline configuration from YAML.

    Resolves the path relative to the project root when not provided.
    Raises FileNotFoundError with a clear message if the file is missing.

    Args:
        config_path: Absolute or relative path to a YAML config file.
                     Defaults to config/pipeline_config.yaml.

    Returns:
        Parsed config dict matching the pipeline_config.yaml structure.

    Raises:
        FileNotFoundError: If the config file does not exist.
        yaml.YAMLError: If the YAML is malformed.
    """
    resolved = Path(config_path) if config_path else _DEFAULT_CONFIG_PATH

    if not resolved.exists():
        raise FileNotFoundError(
            f"Pipeline config not found: {resolved}. "
            "Expected at config/pipeline_config.yaml relative to project root."
        )

    with resolved.open("r", encoding="utf-8") as fh:
        cfg = yaml.safe_load(fh)

    if not isinstance(cfg, dict):
        raise ValueError(f"Config file {resolved} did not parse to a dict (got {type(cfg)}).")

    return cfg


# COMMAND ----------
def get_table_config(config: dict, layer: str, domain: str) -> str:
    """
    Return the fully-qualified table name for a given layer and domain key.

    Args:
        config: Loaded config dict from load_config().
        layer: One of 'bronze', 'silver', 'gold'.
        domain: Domain key as defined under config['tables'][layer].

    Returns:
        Fully-qualified table name string (e.g., 'brz_freshsip.pos_transactions_raw').

    Raises:
        KeyError: If layer or domain key does not exist in config.
    """
    try:
        return config["tables"][layer][domain]
    except KeyError as exc:
        raise KeyError(
            f"Table config not found: tables.{layer}.{domain}. "
            f"Available layers: {list(config.get('tables', {}).keys())}"
        ) from exc


# COMMAND ----------
def get_source_path(config: dict, source_system: str, filename: str) -> str:
    """
    Build the source file path for a given source system and file name.

    Args:
        config: Loaded config dict from load_config().
        source_system: Source system key (e.g., 'erp', 'pos', 'production', 'logistics').
        filename: File name (without path prefix) to append to the base_path.

    Returns:
        Full path string (e.g., 'data/synthetic/erp/orders.csv').

    Raises:
        KeyError: If source_system key is not found in config['sources'].
    """
    try:
        base_path = config["sources"][source_system]["base_path"]
    except KeyError as exc:
        raise KeyError(
            f"Source system '{source_system}' not found in config. "
            f"Available: {list(config.get('sources', {}).keys())}"
        ) from exc

    return os.path.join(base_path, filename)


# COMMAND ----------
def get_layer_database(config: dict, layer: str) -> str:
    """
    Return the Hive database name for a given layer.

    Args:
        config: Loaded config dict from load_config().
        layer: One of 'bronze', 'silver', 'gold'.

    Returns:
        Database name string (e.g., 'brz_freshsip').
    """
    try:
        return config["layers"][layer]["database"]
    except KeyError as exc:
        raise KeyError(
            f"Layer '{layer}' not found in config['layers']. "
            f"Available: {list(config.get('layers', {}).keys())}"
        ) from exc


# COMMAND ----------
def get_dq_threshold(config: dict) -> float:
    """
    Return the DQ failure rate threshold (%) above which a pipeline halts.

    Args:
        config: Loaded config dict from load_config().

    Returns:
        Float percentage (default 5.0 if not configured).
    """
    return float(config.get("thresholds", {}).get("dq_fail_rate_pct", 5.0))
