import argparse
import os

import pandas as pd
from sqlalchemy import create_engine


def create_df_from_csv(url: str) -> pd.DataFrame:

    csv_name = "output.csv"
    os.system(f"wget {url} -O {csv_name}")
    return pd.read_csv(csv_name)

def ingest_data_to_pg(
    user: str,
    password: str,
    host: str,
    port: int,
    db: str,
    table_name: str,
    url: str
) -> None:

    engine = create_engine(f"postgresql://{user}:{password}@{host}:{port}/{db}")
    df = create_df_from_csv(url)
    df.to_sql(name=table_name, con=engine, if_exists="replace")

def main(params):
    user = params.user
    # password = os.environ[params.password]
    password = params.password
    host = params.host
    port = params.port
    database = params.db
    table_name = params.table
    url = params.url

    try:
        ingest_data_to_pg(user, password, host, port, database, table_name, url)
    except Exception as e:
        print(f"\033[91m[ERROR] Data ingestion failed - {str(e)}\033[0m")

if __name__ == "__main__":

    parser = argparse.ArgumentParser(description="Ingest data from CSV file to Postgres.")
    parser.add_argument("--user" , help="Postgres user name")
    parser.add_argument("--password" , help="Postgres user password")
    parser.add_argument("--host" , help="Postgres host")
    parser.add_argument("--port" , help="Postgres port")
    parser.add_argument("--db" , help="Database name")
    parser.add_argument("--table" , help="Table name")
    parser.add_argument("--url" , help="Url to the CSV file")

    args = parser.parse_args()

    main(args)
