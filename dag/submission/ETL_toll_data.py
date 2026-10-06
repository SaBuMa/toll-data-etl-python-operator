"""
ETL_toll_data: toll plaza traffic ETL pipeline built with Airflow PythonOperator.

Downloads the toll data archive, extracts fields from three source formats
(CSV, TSV, fixed-width), consolidates them into one CSV and uppercases the
vehicle type.
"""

# ---------------------------------------------------------------------------
# Libraries
# ---------------------------------------------------------------------------
from datetime import timedelta

# The DAG object; we'll need this to instantiate a DAG
from airflow.models import DAG
# Operators; you need this to write tasks!
from airflow.operators.python import PythonOperator
# This makes scheduling easy
from airflow.utils.dates import days_ago

# Data handling libraries
import os
import tarfile

import pandas as pd
import requests

# ---------------------------------------------------------------------------
# DAG arguments
# ---------------------------------------------------------------------------
default_args = {
    'owner': 'Santiago Burgos',
    'start_date': days_ago(0),
    'email': ['your_email@example.com'],
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# ---------------------------------------------------------------------------
# DAG definition
# ---------------------------------------------------------------------------
dag = DAG(
    'ETL_toll_data',
    default_args=default_args,
    description='Apache Airflow Final Assignment',
    schedule_interval=timedelta(days=1),
)

# ---------------------------------------------------------------------------
# Global variables: input and output file paths
# ---------------------------------------------------------------------------
STAGING = "/home/project/airflow/dags/python_etl/staging"

ARCHIVE = os.path.join(STAGING, "tolldata.tgz")
VEHICLE_DATA = os.path.join(STAGING, "vehicle-data.csv")
TOLLPLAZA_DATA = os.path.join(STAGING, "tollplaza-data.tsv")
PAYMENT_DATA = os.path.join(STAGING, "payment-data.txt")

OUTPUT_CSV = os.path.join(STAGING, "csv_data.csv")
OUTPUT_TSV = os.path.join(STAGING, "tsv_data.csv")
OUTPUT_FIXED_WIDTH = os.path.join(STAGING, "fixed_width_data.csv")
CONSOLIDATED = os.path.join(STAGING, "extracted_data.csv")
TRANSFORMED = os.path.join(STAGING, "transformed_data.csv")

SOURCE_URL = (
    "https://cf-courses-data.s3.us.cloud-object-storage.appdomain.cloud/"
    "IBM-DB0250EN-SkillsNetwork/labs/Final%20Assignment/tolldata.tgz"
)


# ---------------------------------------------------------------------------
# Functions
# ---------------------------------------------------------------------------
def download_dataset():
    """Download the toll data archive into the staging folder."""
    response = requests.get(SOURCE_URL, timeout=30)
    response.raise_for_status()

    with open(ARCHIVE, "wb") as file:
        file.write(response.content)

    print(f"Downloaded {ARCHIVE} ({len(response.content)} bytes) in {response.elapsed}")


def untar_dataset():
    """Extract the archive into the staging folder, skipping macOS '._' metadata files."""
    with tarfile.open(ARCHIVE, "r") as tar:
        members = [
            m for m in tar.getmembers()
            if not os.path.basename(m.name).startswith("._")
        ]
        tar.extractall(STAGING, members=members, filter="data")
        print("Untar completed with files:", [m.name for m in members])


def extract_data_from_csv():
    """Rowid, Timestamp, Anonymized Vehicle number, Vehicle type (columns 0-3)."""
    read_file = pd.read_csv(VEHICLE_DATA, header=None, usecols=[0, 1, 2, 3])
    read_file.to_csv(OUTPUT_CSV, header=False, index=False)
    print("Extracted CSV data shape:", read_file.shape)


def extract_data_from_tsv():
    """Number of axles, Tollplaza id, Tollplaza code (columns 4-6).

    pandas reads the Windows line endings (\\r\\n) of this file as normal
    line breaks, so no carriage return ends up in the Tollplaza code.
    """
    read_file = pd.read_csv(TOLLPLAZA_DATA, header=None, delimiter='\t', usecols=[4, 5, 6])
    read_file.to_csv(OUTPUT_TSV, sep=',', header=False, index=False)
    print("Extracted TSV data shape:", read_file.shape)


def extract_data_from_fixed_width():
    """Type of Payment code and Vehicle Code, read by character position.

    colspecs are 0-based with the end excluded:
    (58, 61) = characters 59-61 and (62, 67) = characters 63-67 in 1-based terms.
    """
    read_file = pd.read_fwf(PAYMENT_DATA, header=None, colspecs=[(58, 61), (62, 67)])
    read_file.to_csv(OUTPUT_FIXED_WIDTH, header=False, index=False)
    print("Extracted fixed-width data shape:", read_file.shape)


def consolidate_data():
    """Combine the three extracts side by side into one 9-column CSV."""
    files_list = [
        pd.read_csv(OUTPUT_CSV, header=None),
        pd.read_csv(OUTPUT_TSV, header=None),
        pd.read_csv(OUTPUT_FIXED_WIDTH, header=None),
    ]
    read_file = pd.concat(files_list, axis=1)
    read_file.to_csv(CONSOLIDATED, header=False, index=False)
    print("Consolidated data shape:", read_file.shape)


def transform_data():
    """Uppercase the vehicle type (column 3), keeping every other column intact."""
    read_file = pd.read_csv(CONSOLIDATED, header=None)
    read_file[3] = read_file[3].str.upper()
    read_file.to_csv(TRANSFORMED, header=False, index=False)
    print("Transformed data shape:", read_file.shape)


# ---------------------------------------------------------------------------
# Tasks
# ---------------------------------------------------------------------------
# Download the source archive into staging
task_download_dataset = PythonOperator(
    task_id="download_dataset",
    python_callable=download_dataset,
    dag=dag,
)

# Untar the source archive
task_untar_dataset = PythonOperator(
    task_id="untar_dataset",
    python_callable=untar_dataset,
    dag=dag,
)

# CSV source: Rowid, Timestamp, Anonymized Vehicle number, Vehicle type
task_extract_data_from_csv = PythonOperator(
    task_id="extract_data_from_csv",
    python_callable=extract_data_from_csv,
    dag=dag,
)

# TSV source: Number of axles, Tollplaza id, Tollplaza code
task_extract_data_from_tsv = PythonOperator(
    task_id="extract_data_from_tsv",
    python_callable=extract_data_from_tsv,
    dag=dag,
)

# Fixed-width source: Type of Payment code, Vehicle Code
task_extract_data_from_fixed_width = PythonOperator(
    task_id="extract_data_from_fixed_width",
    python_callable=extract_data_from_fixed_width,
    dag=dag,
)

# Merge the three extracts side by side, line by line
task_consolidate_data = PythonOperator(
    task_id="consolidate_data",
    python_callable=consolidate_data,
    dag=dag,
)

# Uppercase the vehicle type, keeping every other column intact
task_transform_data = PythonOperator(
    task_id="transform_data",
    python_callable=transform_data,
    dag=dag,
)

# ---------------------------------------------------------------------------
# Task pipeline
# ---------------------------------------------------------------------------
(
    task_download_dataset
    >> task_untar_dataset
    >> task_extract_data_from_csv
    >> task_extract_data_from_tsv
    >> task_extract_data_from_fixed_width
    >> task_consolidate_data
    >> task_transform_data
)
