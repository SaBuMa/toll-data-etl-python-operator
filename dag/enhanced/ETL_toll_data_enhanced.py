"""
ETL_toll_data_enhanced: portfolio version of the toll data pipeline.

Same seven steps as the course DAG, plus:
  * Parallel extraction (fan-out / fan-in): the three extract tasks read and
    write different files, so they run at the same time
  * A validation task that checks the final file before anyone uses it
  * Row-count guard in consolidate_data (pd.concat aligns by index, so the
    three extracts must have the same number of rows)
  * Staging folder configurable through the TOLL_STAGING environment variable
  * Imports that work on both Airflow 2.x and Airflow 3.x
"""
import os
import tarfile
from datetime import datetime, timedelta

import pandas as pd
import requests

# --- Airflow 2 / Airflow 3 compatible imports -------------------------------
try:  # Airflow 3
    from airflow.sdk import DAG
except ImportError:  # Airflow 2
    from airflow import DAG

try:  # Airflow 3 (standard provider)
    from airflow.providers.standard.operators.python import PythonOperator
except ImportError:  # Airflow 2
    from airflow.operators.python import PythonOperator

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------
STAGING = os.getenv("TOLL_STAGING", "/home/project/airflow/dags/python_etl/staging")

SOURCE_URL = (
    "https://cf-courses-data.s3.us.cloud-object-storage.appdomain.cloud/"
    "IBM-DB0250EN-SkillsNetwork/labs/Final%20Assignment/tolldata.tgz"
)

ARCHIVE = os.path.join(STAGING, "tolldata.tgz")
VEHICLE_DATA = os.path.join(STAGING, "vehicle-data.csv")
TOLLPLAZA_DATA = os.path.join(STAGING, "tollplaza-data.tsv")
PAYMENT_DATA = os.path.join(STAGING, "payment-data.txt")

OUTPUT_CSV = os.path.join(STAGING, "csv_data.csv")
OUTPUT_TSV = os.path.join(STAGING, "tsv_data.csv")
OUTPUT_FIXED_WIDTH = os.path.join(STAGING, "fixed_width_data.csv")
CONSOLIDATED = os.path.join(STAGING, "extracted_data.csv")
TRANSFORMED = os.path.join(STAGING, "transformed_data.csv")

# Fixed-width positions: 0-based, end excluded
# (58, 61) = characters 59-61, (62, 67) = characters 63-67 in 1-based terms
FIXED_WIDTH_COLSPECS = [(58, 61), (62, 67)]

EXPECTED_COLUMNS = 9
VEHICLE_TYPE_COLUMN = 3


# ---------------------------------------------------------------------------
# Functions (each one is a "recipe card" handed to a PythonOperator)
# ---------------------------------------------------------------------------
def download_dataset():
    """Download the toll data archive into the staging folder."""
    os.makedirs(STAGING, exist_ok=True)
    response = requests.get(SOURCE_URL, timeout=30)
    response.raise_for_status()  # 4xx/5xx -> HTTPError -> task fails -> retry

    with open(ARCHIVE, "wb") as file:
        file.write(response.content)

    print(f"Downloaded {ARCHIVE} ({len(response.content)} bytes) in {response.elapsed}")


def untar_dataset():
    """Extract the archive, skipping macOS '._' metadata files."""
    with tarfile.open(ARCHIVE, "r") as tar:
        members = [
            m for m in tar.getmembers()
            if not os.path.basename(m.name).startswith("._")
        ]
        tar.extractall(STAGING, members=members, filter="data")
        print("Untar completed with files:", [m.name for m in members])


def extract_data_from_csv():
    """Rowid, Timestamp, Anonymized Vehicle number, Vehicle type (columns 0-3)."""
    df = pd.read_csv(VEHICLE_DATA, header=None, usecols=[0, 1, 2, 3])
    df.to_csv(OUTPUT_CSV, header=False, index=False)
    print(f"Extracted {df.shape[0]} rows x {df.shape[1]} columns into {OUTPUT_CSV}")


def extract_data_from_tsv():
    """Number of axles, Tollplaza id, Tollplaza code (columns 4-6).

    The source has Windows line endings (\\r\\n); pandas treats them as normal
    line breaks, so no carriage return ends up in the Tollplaza code.
    """
    df = pd.read_csv(TOLLPLAZA_DATA, header=None, sep="\t", usecols=[4, 5, 6])
    df.to_csv(OUTPUT_TSV, sep=",", header=False, index=False)
    print(f"Extracted {df.shape[0]} rows x {df.shape[1]} columns into {OUTPUT_TSV}")


def extract_data_from_fixed_width():
    """Type of Payment code and Vehicle Code, read by character position."""
    df = pd.read_fwf(PAYMENT_DATA, header=None, colspecs=FIXED_WIDTH_COLSPECS)
    df.to_csv(OUTPUT_FIXED_WIDTH, header=False, index=False)
    print(f"Extracted {df.shape[0]} rows x {df.shape[1]} columns into {OUTPUT_FIXED_WIDTH}")


def consolidate_data():
    """Combine the three extracts side by side into one 9-column CSV."""
    parts = [pd.read_csv(path, header=None)
             for path in (OUTPUT_CSV, OUTPUT_TSV, OUTPUT_FIXED_WIDTH)]

    # pd.concat(axis=1) aligns rows by index label: refuse to glue
    # files of different lengths instead of silently creating empty cells.
    row_counts = [len(p) for p in parts]
    if len(set(row_counts)) != 1:
        raise ValueError(f"Extracts have different row counts: {row_counts}")

    df = pd.concat(parts, axis=1)
    df.to_csv(CONSOLIDATED, header=False, index=False)
    print(f"Consolidated {df.shape[0]} rows x {df.shape[1]} columns into {CONSOLIDATED}")


def transform_data():
    """Uppercase the vehicle type, keeping every other column intact."""
    df = pd.read_csv(CONSOLIDATED, header=None)
    df[VEHICLE_TYPE_COLUMN] = df[VEHICLE_TYPE_COLUMN].str.upper()
    df.to_csv(TRANSFORMED, header=False, index=False)
    print(f"Transformed {df.shape[0]} rows x {df.shape[1]} columns into {TRANSFORMED}")


def validate_output():
    """Check the final file: shape, no missing values, uppercase vehicle types."""
    df = pd.read_csv(TRANSFORMED, header=None)
    errors = []

    if df.shape[1] != EXPECTED_COLUMNS:
        errors.append(f"expected {EXPECTED_COLUMNS} columns, got {df.shape[1]}")
    if df.empty:
        errors.append("file has no rows")
    if df.isna().any().any():
        errors.append(f"{int(df.isna().sum().sum())} missing values")

    vehicle_types = df[VEHICLE_TYPE_COLUMN].astype(str)
    if not (vehicle_types == vehicle_types.str.upper()).all():
        errors.append("some vehicle types are not uppercase")

    if errors:
        raise ValueError("Validation failed: " + "; ".join(errors))

    counts = vehicle_types.value_counts().to_dict()
    print(f"Validation passed: {df.shape[0]} rows x {df.shape[1]} columns, vehicle types {counts}")


# ---------------------------------------------------------------------------
# DAG
# ---------------------------------------------------------------------------
default_args = {
    "owner": "Santiago Burgos",
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    dag_id="ETL_toll_data_enhanced",
    description="Toll data ETL with PythonOperator: parallel extraction and validation",
    default_args=default_args,
    start_date=datetime(2026, 10, 1),
    schedule=timedelta(days=1),
    catchup=False,
    tags=["etl", "toll-data", "python-operator", "portfolio"],
) as dag:

    task_download_dataset = PythonOperator(task_id="download_dataset", python_callable=download_dataset)
    task_untar_dataset = PythonOperator(task_id="untar_dataset", python_callable=untar_dataset)

    task_extract_csv = PythonOperator(task_id="extract_data_from_csv", python_callable=extract_data_from_csv)
    task_extract_tsv = PythonOperator(task_id="extract_data_from_tsv", python_callable=extract_data_from_tsv)
    task_extract_fixed = PythonOperator(
        task_id="extract_data_from_fixed_width", python_callable=extract_data_from_fixed_width
    )

    task_consolidate_data = PythonOperator(task_id="consolidate_data", python_callable=consolidate_data)
    task_transform_data = PythonOperator(task_id="transform_data", python_callable=transform_data)
    task_validate_output = PythonOperator(task_id="validate_output", python_callable=validate_output)

    # Fan-out / fan-in: the three extracts touch different files, so they run in parallel
    (
        task_download_dataset
        >> task_untar_dataset
        >> [task_extract_csv, task_extract_tsv, task_extract_fixed]
        >> task_consolidate_data
        >> task_transform_data
        >> task_validate_output
    )
