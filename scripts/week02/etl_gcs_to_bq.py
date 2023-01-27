from pathlib import Path
from random import randint

import pandas as pd
from prefect import flow, task
from prefect_gcp import GcpCredentials
from prefect_gcp.cloud_storage import GcsBucket


@task(retries=3)
def extract_from_gcs(
    color: str, year: int, month: int, folder: str, local_path
) -> Path:
    """Extract data from gcs"""

    gcs_path = f"{folder}/{color}/{color}_tripdata_{year}-{month}.parquet"
    gcs_block = GcsBucket.load("zoomcamp-gcs")

    gcs_block.get_directory(from_path=gcs_path, local_path=local_path)

    return Path(f"{gcs_path}")

@task()
def transform(path: Path) -> pd.DataFrame:
    """Cleaning task example"""

    df = pd.read_parquet(path)
    print(f"\033[91mPRE: missing passenger count: {df['passenger_count'].isna().sum()}\033[0m")
    df['passenger_count'] = df['passenger_count'].fillna(0)
    print(f"\033[92mPOST: missing passenger count: {df['passenger_count'].isna().sum()}\033[0m")

    return df

@task(retries=3)
def load_to_bq(df: pd.DataFrame) -> None:
    """Load DataFrame to BigQuery dataset"""

    gcp_credentials_block = GcpCredentials.load("zoomcamp-gcp-credentials")

    df.to_gbq(
        destination_table="de_zoocamp_2023.yellow_tripdata_2021_01", 
        project_id="light-reality-344611", 
        credentials=gcp_credentials_block.get_credentials_from_service_account(),
        chunksize=500_000,
        if_exists="append"
    )

@flow()
def etl_gcs_to_bq():
    """main ETL flow to load data from GCS to BigQuery"""
    color = "yellow"
    year = 2021
    month = "01"
    folder = "data"
    local_path = "."

    path = extract_from_gcs(color, year, month, folder, local_path)
    df = transform(path)
    load_to_bq(df)

if __name__ == "__main__":
    etl_gcs_to_bq()