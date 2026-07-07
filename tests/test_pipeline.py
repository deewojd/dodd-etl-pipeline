"""
tests/test_pipeline.py

DODD Unit Tests: Data Observability-Driven Development
Tests are written alongside (or before) the pipeline logic,
making observability a design constraint, not an afterthought.
"""

import pytest
import pandas as pd
import sys
import os

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from dodd_etl_pipeline import transform, validate, REQUIRED_COLUMNS, VALUE_RULES
from observability import (
    validate_schema,
    check_row_count,
    check_nulls,
    check_value_ranges,
    log_anomaly
)

# Fixtures
@pytest.fixture
def valid_df():
    """A valid DataFrame representing ideal input."""
    return pd.DataFrame({
        "name": ["Apple", "Banana", "Carrot"],
        "calories": [52.0, 89.0, 41.0],
        "total_fat": [0.2, 0.3, 0.2],
        "protein": [0.3, 1.1, 0.9],
        "carbohydrate": [14.0, 23.0, 10.0],
        "sugars": [10.0, 12.0, 5.0],
        "fiber": [2.4, 2.6, 2.8]
    })

@pytest.fixture
def df_with_nulls(valid_df):
    df = valid_df.copy()
    df.loc[0, "calories"] = None
    df.loc[1, "protein"] = None
    return df

@pytest.fixture
def df_with_duplicates(valid_df):
    return pd.concat([valid_df, valid_df.iloc[[0]]], ignore_index=True)

@pytest.fixture
def df_with_out_of_range(valid_df):
    df = valid_df.copy()
    df.loc[0,"calories"] = 9999  # Top range
    df.loc[1, "total_fat"] = -5  # Bottom range
    return df

@pytest.fixture
def empty_df():
    return pd.DataFrame(columns=REQUIRED_COLUMNS)

# Schema Tests
class TestSchemaValidation:

    def test_valid_schema_passes(self, valid_df):
        """All required columns present, should not raise."""
        validate_schema(valid_df, REQUIRED_COLUMNS)

    def test_missing_column_raises(self, valid_df):
        """Missing a required column should raise ValueError."""
        df = valid_df.drop(columns=["calories"])
        with pytest.raises(ValueError, match="Schema violation"):
            validate_schema(df, REQUIRED_COLUMNS)

    def test_multiple_missing_columns_raises(self, valid_df):
        df = valid_df.drop(columns=["calories", "protein"])
        with pytest.raises(ValueError, match="Schema violation"):
            validate_schema(df, REQUIRED_COLUMNS)

# Row Count Tests
class TestRowCount:
    def test_sufficent_rows_passes(self, valid_df):
        check_row_count(valid_df, min_rows=1, stage="test")

    def test_empty_dataframe_raises(self, empty_df):
        with pytest.raises(ValueError, match="Row count check failed"):
            check_row_count(empty_df, min_rows=1, stage="test")

    def test_transform_output_is_not_empty(self, valid_df):
        result = transform(valid_df)
        assert len(result) > 0, "Transform output should not be empty"

# Null Tests
class TestNullChecks:
    def test_no_nulls_passes(self, valid_df):
        """Clean DataFrame should pass null checks silently"""
        check_nulls(valid_df, critical_columns=REQUIRED_COLUMNS, stage="test")

    def test_null_in_name_is_dropped_by_transform(self):
        """Rows with null names should be dropped during transform"""
        df = pd.DataFrame({
            "name": [None, "Banana"],
            "calories": [52.0, 89.0],
            "total_fat": [0.2, 0.3],
            "protein": [0.3, 1.1],
            "carbohydrate": [14.0, 23.0],
            "sugars": [10.0, 12.0],
            "fiber": [2.4, 2.6]
        })
        result = transform(df)
        assert result["name"].isna().sum() == 0
        
    def test_null_numerics_filled_with_zero(self, df_with_nulls):
        """Null numeric values should be filled with 0 after transform"""
        result = transform(df_with_nulls)
        numeric_cols = [c for c in REQUIRED_COLUMNS if c!= "name"]
        for col in numeric_cols:
            assert result[col].isna().sum() == 0, f"Column '{col}' still has nulls after transform"

    def test_output_has_no_nulls_in_critical_columns(self, valid_df):
        result = transform(valid_df)
        for col in REQUIRED_COLUMNS:
            assert result[col].isna().sum() == 0

# Duplicate Tests
class TestDeduplication:

    def test_duplicates_are_removed(self, df_with_duplicates):
        result = transform(df_with_duplicates)
        assert result["name"].duplicated().sum() == 0

    def test_unique_rows_are_preserved(self, valid_df):
        result = transform(valid_df)
        assert len(result) == len(valid_df)

# Value Range Tests
class TestValueRanges:

    def test_valid_ranges_pass(self, valid_df):
        check_value_ranges(valid_df, VALUE_RULES, stage="test")

    def test_out_of_range_values_are_clipped(self, df_with_out_of_range):
        result = transform(df_with_out_of_range)
        for col, (min_val, max_val) in VALUE_RULES.items():
            assert result[col].min() >= min_val, f"{col} below minimum after transform"
            assert result[col].max() <= max_val, f"{col} above maximum after transform"
    def test_negative_calories_clipped_to_zero(self):
        df = pd.DataFrame({
            "name": ["Mystery Food"],
            "calories": [-100.0],
            "total_fat": [0.0],
            "protein": [0.0],
            "carbohydrate": [0.0],
            "sugars": [0.0],
            "fiber": [0.0]
            })
        result = transform(df)
        assert result["calories"].iloc[0] >= 0

# End-to-end Transfrom Tests
class TestTransform:
    def test_output_columns_match_required(self, valid_df):
        result = transform(valid_df)
        for col in REQUIRED_COLUMNS:
            assert col in result.columns

    def test_column_names_are_normalized(self):
        """Column names with spaces/caps should be normalized."""
        df = pd.DataFrame({
            "Name": ["Apple"],
            "Calories": [52.0],
            "Total_Fat": [0.2],
            "Protein": [0.3],
            "Carbohydrate": [14.0],
            "Sugars": [10.0],
            "Fiber": [2.4],
            })
        result = transform(df)
        for col in result.columns:
            assert col == col.lower(), f"Column '{col}' is not lowercase"
