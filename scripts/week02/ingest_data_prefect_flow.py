import argparse
import io
from datetime import timedelta

import pandas as pd
import requests
from prefect import flow, task
from prefect.tasks import task_input_hash
from prefect_sqlalchemy import SqlAlchemyConnector
from sqlalchemy import create_engine


@task(
    name="Extracting Data",
    log_prints=True, retries=3,
    cache_key_fn=task_input_hash,
    cache_expiration=timedelta(days=1)
)
def extract_data(url: str) -> pd.DataFrame:

    if url.endswith(".csv.gz"):
        csv_name = "yellow_tripdata_2021-01.csv.gz"
    else:
        csv_name = "output.csv"

    response = requests.get(url)
    with open(csv_name, 'wb') as f:
        f.write(response.content)
    return pd.read_csv(csv_name)

@task(name="Transforming Data", log_prints=True, retries=3)
def tranform(df: pd.DataFrame) -> pd.DataFrame:

    if 'passenger_count' in df.columns:
        print(f"\033[93m[PRE] - Missing passengers count: {df['passenger_count'].isin([0]).sum()}.\033[0m")
        df = df[df['passenger_count'] != 0]
        print(f"\033[92m[POS] - Missing passengers count: {df['passenger_count'].isin([0]).sum()}.\033[0m")
        return df
    else:
        return df
    

@task(log_prints=True, retries=3)
def ingest_data_to_pg(
    # user: str,
    # password: str,
    # host: str,
    # port: int,
    # db: str,
    table_name: str,
    df: pd.DataFrame
) -> None:

    connection_block = SqlAlchemyConnector.load("postgres-connector")

    with connection_block.get_connection(begin=False) as engine:
        # engine = create_engine(f"postgresql://{user}:{password}@{host}:{port}/{db}")
        # with requests.get(url) as response:
        #     df = pd.read_csv(io.StringIO(response.text))

        df.to_sql(name=table_name, con=engine, if_exists="replace")

@flow(name="Ingest Zoocamp Data")
def main(params):
    # user = params.user
    # password = params.password
    # host = params.host
    # port = params.port
    # database = params.db
    table_name = params.table
    url = params.url

    try:
        raw_data = extract_data(url)
    except Exception as e:
        print(f"\033[91m[ERROR] Data extraction failed - {str(e)}\033[0m")

    try:
        transformed_data = tranform(raw_data)
    except Exception as e:
        print(f"\033[91m[ERROR] Data transformation failed - {str(e)}\033[0m")

    try:
        # ingest_data_to_pg(user, password, host, port, database, table_name, transformed_data)
        ingest_data_to_pg(table_name, transformed_data)
    except Exception as e:
        print(f"\033[91m[ERROR] Data ingestion failed - {str(e)}\033[0m")

def main_flow():
    parser = argparse.ArgumentParser(description="Ingest data from CSV file to Postgres.")
    # parser.add_argument("--user" , help="Postgres user name")
    # parser.add_argument("--password" , help="Postgres user password")
    # parser.add_argument("--host" , help="Postgres host")
    # parser.add_argument("--port" , help="Postgres port")
    # parser.add_argument("--db" , help="Database name")
    parser.add_argument("--table" , help="Table name")
    parser.add_argument("--url" , help="Url to the CSV file")

    args = parser.parse_args()

    main(args)

if __name__ == "__main__":
    main_flow()
