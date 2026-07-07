# DODD ETL Pipeline: USDA Food Nutrition Data

An example of **Data Observability-Driven Development (DODD)** a philosophy inspired by Test-Driven Development (TDD) that treats observability as a priority.

---

## What is DODD?

In traditional data enginerring, observability is many times reactive. One adds logging and alerts **after** something breaks in production. DODD flips this.

> **Design the observability layer first. Build the pipeline around it.**

This means:
- Schema validation runs **before** any transformation touches the data
- Row counts, null checks, and value range checks run at **every stage**
- Anomalies are **logged and surfaced**
- Unit tests validate each observable property independently

This project is inspired by the concept introduced in **Fundamentals of Data Engineering** by Joe Reis and Matt Housley.

---

## Project Structure

```
dodd_nutrition/
├── data/
│   └── nutrition.csv          # Raw USDA nutrition data (source)
├── output/
│   └── nutrition_clean.csv    # Pipeline output (generated)
├── logs/
│   └── pipeline_YYYYMMDD.log  # Observability logs (generated)
├── tests/
│   └── test_pipeline.py       # DODD unit test suite
├── dodd_etl_pipeline.py       # ETL pipeline (Extract → Validate → Transform → Load)
├── observability.py           # Observability layer (schema, nulls, ranges, anomalies)
├── requirements.txt
└── README.md
```

---

## Data Engineering Lifecycle Mapping

| Stage        | Component              | Description                                      |
|--------------|------------------------|--------------------------------------------------|
| **Ingestion**    | `extract()`        | Reads raw CSV from source                        |
| **Storage**      | `data/`            | Raw source data at rest                          |
| **Transformation** | `transform()`   | Cleans, normalizes, deduplicates                 |
| **Observability**  | `observability.py` | Schema, nulls, ranges, anomaly logging       |
| **Serving**      | `output/`          | Clean CSV ready for analytics or downstream use  |

---

## Observability Checks

| Check | Stage | Behavior |
|---|---|---|
| Schema validation | Pre-transform | Raises if required columns are missing |
| Row count | Pre & post transform | Raises if DataFrame is empty |
| Null check | Pre & post transform | Logs warning; transform fills or drops |
| Value range check | Pre & post transform | Logs warning; transform clips to bounds |
| Duplicate check | Transform | Logs warning; deduplicates on `name` |

All anomalies route through `log_anomaly()`: a single function designed to be extended with alerting (Slack, PagerDuty, email) without touching pipeline logic.

---

## Unit Tests

Tests are organized by observable property, mirroring DODD's philosophy that each data quality concern deserves its own explicit assertion.

```
tests/test_pipeline.py
├── TestSchemaValidation     # Required columns present
├── TestRowCount             # No empty DataFrames
├── TestNullChecks           # Nulls handled correctly at each stage
├── TestDeduplication        # No duplicate names in output
├── TestValueRanges          # Numeric values within expected bounds
└── TestTransform            # End-to-end transform correctness
```

---

## Getting Started

### 1. Install dependencies
```bash
pip install -r requirements.txt
```

### 2. Run the pipeline
```bash
python dodd_etl_pipeline.py
```

### 3. Run the test suite
```bash
pytest tests/ -v
```

### 4. Check the logs
```bash
cat logs/pipeline_<timestamp>.log
```

---

## Example Output

```
2026-06-24 [INFO] === Pipeline started ===
2026-06-24 [INFO] Extracting data from data/nutrition.csv
2026-06-24 [INFO] Extracted 20 rows, 7 columns
2026-06-24 [INFO] Schema validation passed: all required columns present
2026-06-24 [INFO] Row count check passed at 'extract': 20 rows
2026-06-24 [INFO] Null check passed at 'extract': 'name' has no nulls
2026-06-24 [INFO] Transform complete: 20 rows remaining
2026-06-24 [INFO] Loaded 20 rows to output/nutrition_clean.csv
2026-06-24 [INFO] === Pipeline complete ===
```

---

## Key Takeaway

The `observability.py` module is intentionally decoupled from `pipeline.py`. This means:
- Observability logic can be tested independently
- New checks can be added without modifying pipeline logic
- The anomaly logger can be extended to fire real alerts with one change
