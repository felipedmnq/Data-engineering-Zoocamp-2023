from datetime import timedelta
from pathlib import Path
from random import randint
from typing import List

import pandas as pd
from prefect import flow, task
from prefect.tasks import task_input_hash
from prefect_gcp.cloud_storage import GcsBucket


@task(retries=3, cache_key_fn=task_input_hash, cache_expiration=timedelta(days=1))
def fetch(dataset_url: str) -> pd.DataFrame:
    """Read taxi data from WEB into pandas DataFrame."""

    # # retries test
    # if randint(0, 1) > 0:
    #     raise Exception

    return pd.read_csv(dataset_url)

@task(log_prints=True)
def clean_data(df: pd.DataFrame) -> pd.DataFrame:
    """Fix dtypes"""

    if "tpep_pickup_datetime" in df.columns and "tpep_dropoff_datetime" in df.columns:
        df["tpep_pickup_datetime"] = pd.to_datetime(df["tpep_pickup_datetime"])
        df["tpep_dropoff_datetime"] = pd.to_datetime(df["tpep_dropoff_datetime"])
        return df

    else:
        return df

@task()
def write_local(df: pd.DataFrame, color: str, dataset_file: str) -> Path:
    """Write DataFrame out as parquet file"""

    path = Path(f"data/{color}/{dataset_file}.parquet")
    df.to_parquet(path, compression="gzip")

    return path

@task()
def write_gcs(path: Path) -> None:
    """Uploading local parquet file to GCS"""

    gcp_block = GcsBucket.load("zoomcamp-gcs")
    gcp_block.upload_from_path(
        from_path=f"{path}",
        to_path=path
)

@flow()
def etl_web_to_gcs(color: str, year: int, month: int) -> None:
    """Main ETL function"""

    dataset_file = f"{color}_tripdata_{year}-{month:02}"
    dataset_url = f"https://github.com/DataTalksClub/nyc-tlc-data/releases/download/{color}/{dataset_file}.csv.gz"

    df = fetch(dataset_url)
    df_clean = clean_data(df)
    path = write_local(df_clean, color, dataset_file)
    write_gcs(path)

@flow()
def etl_parent_flow(
    color: str = "yellow", year: int = 2021, months: List[int] = [1, 2]
) -> None:

    for month in months:
        etl_web_to_gcs(color, year, month)

if __name__ == "__main__":
    color = "yellow"
    year = 2021
    months = [1, 2, 3]

    etl_parent_flow(color, year, months)

