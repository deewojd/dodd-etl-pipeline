"""
observability.

Foundation of DODD: Data Observability-Driven Development. These functions prioritize
data quality. Anomalies are logged and surfaced.
"""

import logging
import pandas as pd

logger = logging.getLogger(__name__)

def log_anomaly(message: str, level: str = "WARNING") -> None:

    """
    Main anomaly logger. Al data quality issues route through here. Therefore
    easy to plug in alerting (Slack, PagerDuty, etc.) later.
    """
    if level == "WARNING":
        logger.warning(f"[ANOMALY] {message}")
    elif level == "ERROR":
        logger.error(f"[ANOMALY] {message}")
        raise ValueError(f"Critical anomaly detected: {message}")
    else:
        logger.info(f"[ANOMALY] {message}")

def validate_schema(df: pd.DataFrame, required_columns: list) -> None:
    """
    Validates that all required columns are present. Immediately noticed if
    schema is violated. Downstream transformations cannot be trusted if this is skipped.
    """
    missing = [col for col in required_columns if col not in df.columns]
    if missing:
        log_anomaly(f"Schema violation. Missing columns: {missing}", level="ERROR")
    else:
        logger.info(f"Schema validation passed: all required columns present")

def check_row_count(df: pd.DataFrame, min_rows: int, stage: str) -> None:
    if len(df) < min_rows:
        log_anomaly(
            f"Row count check failed at '{stage}': expected >={min_rows}, got {len(df)}",
            level="ERROR"
        )
    else:
        logger.info(f"Row count check passed at '{stage}': {len(df)} rows")

def check_nulls(df: pd.DataFrame, critical_columns: list, stage: str) -> None:
    """
    Checks for nulls in critical columns at a given stage. Logs each violation, does not raise, allowing the pipeline
    to continue and handle nulls in the transfrom step.
    """
    for col in critical_columns:
        if col not in df.columns:
            continue
        null_count = df[col].isna().sum()
        if null_count > 0:
            log_anomaly(
                f"Null check at '{stage}': {null_count} null(s) found in '{col}'",
                level="WARNING"
            )
        else:
            logger.info(f"Null check passed at '{stage}': '{col}' has no nulls")

def check_value_ranges(df: pd.DataFrame, rules: dict, stage: str) -> None:
    """
    Confirms that numeric columns fall within expected bounds. Out-of-range values are logged as anomalies. Rules format
    {"column_name": (min, max)}
    """
    for col, (min_val, max_val) in rules.items():
        if col not in df.columns:
            continue
        violations = df[(df[col] < min_val) | (df[col] > max_val)]
        if not violations.empty:
            log_anomaly(
                f"Range check at '{stage}': {len(violations)} value(s) in '{col}' "
                f"outside [{min_val}, {max_val}]",
                level="WARNING"
                )
        else:
            logger.info(f"Range check passed at '{stage}': '{col}' within [{min_val}, {max_val}]")
