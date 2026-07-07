
"""
DODD ETL Pipeline: USDA Food Nutrition Data
Data Observability-Driven Development example

Observability prioritized:
- Anomalies are logged, not silently dropped
- Schema is validated before transformation
- Row counts and null checks run at every stage
"""

import os
import logging
import pandas as pd
from datetime import datetime
from observability import validate_schema, check_row_count, check_nulls, check_value_ranges, log_anomaly

# Logging setup
logger = logging.getLogger(__name__)

def setup_logging():

    root = logging.getLogger()
    if root.handlers:
        return

    os.makedirs("logs", exist_ok=True)

    file_handler = logging.FileHandler(
        f"logs/pipeline_{datetime.now().strftime('%Y%m%d_%H%M%S')}.log",
        encoding="utf-8",
        delay=True,
    )

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
        handlers=[file_handler, logging.StreamHandler()],
        force=True
    )

# Config
INPUT_PATH = "data/nutrition.csv"
OUTPUT_PATH = "output/nutrition_clean.csv"

REQUIRED_COLUMNS = [
    "name", "calories", "total_fat", "protein", "carbohydrate", "sugars", "fiber"
]

VALUE_RULES = {
    "calories": (0, 900),
    "total_fat": (0, 900),
    "protein": (0, 100),
    "carbohydrate": (0,100),
    "sugars": (0, 100),
    "fiber": (0, 100) 
}

# Extract
def extract(path: str) -> pd.DataFrame:
    logger.info(f"Extracting data from {path}")
    df = pd.read_csv(path, dtype={col: float for col in REQUIRED_COLUMNS if col != "name"})
    logger.info(f"Extracted {len(df)} rows, {len(df.columns)} columns")
    return df

# Validate
def validate(df: pd.DataFrame) -> pd.DataFrame:
    logger.info("Running pre-transform validation (DODD observability checks)")
    validate_schema(df, REQUIRED_COLUMNS)
    check_row_count(df, min_rows=1, stage="extract")
    check_nulls(df, critical_columns=["name", "calories"], stage="extract")
    check_value_ranges(df, VALUE_RULES, stage="extract")

    return df

# Transform
def transform(df: pd.DataFrame) -> pd.DataFrame:
    logger.info("Transforming data")

    # Normalize column names
    df.columns = [c.strip().lower().replace(" ", "_") for c in df.columns]

    # Select and reorder columns
    df = df[[c for c in REQUIRED_COLUMNS if c in df.columns]].copy()

    # Drop rows with null names
    before = len(df)
    df = df.dropna(subset=["name"])
    dropped = before - len(df)
    if dropped > 0:
        log_anomaly(f"Dropped {dropped} rows with null 'name' values", level="WARNING")

    # Fill numeric nulls with 0 and log
    numeric_cols = [c for c in REQUIRED_COLUMNS if c != "name"]
    for col in numeric_cols:
        null_count = df[col].isna().sum()
        if null_count > 0:
            log_anomaly(f"Filled {null_count} null(s) in '{col}' with 0", level="WARNING")
            df[col] = df[col].fillna(0)

    # Clip out-of-range values and log
    for col, (min_val, max_val) in VALUE_RULES.items():
        if col in df.columns:
            out_of_range = df[(df[col] < min_val) | (df[col] > max_val)]
            if not out_of_range.empty:
                log_anomaly(
                    f"{len(out_of_range)} value(s) in '{col}' outside [{min_val}, {max_val}] - clipping", level="WARNING")
                df[col] = df[col].clip(lower=min_val, upper=max_val)

    # Deduplicate
    before = len(df)
    df = df.drop_duplicates(subset=["name"])
    dupes = before - len(df)
    if dupes > 0:
        log_anomaly(f"Removed {dupes} duplicate 'name' row(s)", level = "WARNING")

    logger.info(f"Transform complete: {len(df)} rows remaining")
    return df

# Post-transform validation
def validate_output(df: pd.DataFrame) -> pd.DataFrame:
    logger.info("Running post-transform validation")
    check_row_count(df, min_rows=1, stage="transform")
    check_nulls(df, critical_columns=REQUIRED_COLUMNS, stage="transform")
    check_value_ranges(df, VALUE_RULES, stage="transform")
    return df

# Load
def load(df: pd.DataFrame, path: str) -> None:
    os.makedirs(os.path.dirname(path), exist_ok=True)
    df.to_csv(path, index=False, float_format="%.1f")
    logger.info(f"Loaded {len(df)} rows to {path}")

# Orchestrate
def run():
    setup_logging()
    logger.info("=== Pipeline started ===")

    try:
        df = extract(INPUT_PATH)
        df = validate(df)
        df = transform(df)
        df = validate_output(df)
        load(df, OUTPUT_PATH)

        logger.info("=== Pipeline complete ===")

    except Exception:
        logger.exception("Pipeline failed")
        raise

if __name__ == "__main__":
    run()
