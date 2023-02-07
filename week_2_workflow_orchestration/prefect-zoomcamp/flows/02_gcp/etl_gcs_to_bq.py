from pathlib import Path
import pandas as pd
from prefect import flow, task
from prefect_gcp.cloud_storage import GcsBucket
from prefect_gcp import GcpCredentials


@task(retries=3)
def extract_from_gcs(color: str, year: int, month: int) -> Path:
    """Download trip data from GCS"""
    gcs_path = "data"
    gcs_block = GcsBucket.load("zoom-gcs")
    localpath = Path(__file__).parent 
    gcs_block.get_directory(from_path=gcs_path, local_path=localpath)
     # to download a single file, use download_object_to_path. # see https://prefecthq.github.io/prefect-gcp/cloud_storage/#prefect_gcp.cloud_storage.GcsBucket.download_object_to_path
    return localpath / f"data/{color}/{color}_tripdata_{year}-{month:02}.parquet"


@task()
def transform(path: Path) -> pd.DataFrame:
    """Data cleaning example"""
    df = pd.read_parquet(path)
    print(f"pre: missing passenger count: {df['passenger_count'].isna().sum()}")
    df["passenger_count"].fillna(0, inplace=True)
    print(f"post: missing passenger count: {df['passenger_count'].isna().sum()}")
    return df


@task()
def write_bq(df: pd.DataFrame) -> None:
    """Write DataFrame to BiqQuery"""

    gcp_credentials_block = GcpCredentials.load("zoom-gcp-creds")

    df.to_gbq(
        destination_table="dezoomcamp.rides",
        project_id="dtc-de-375211",
        credentials=gcp_credentials_block.get_credentials_from_service_account(),
        chunksize=500_000,
        if_exists="append",
    )
    return


@flow()
def etl_gcs_to_bq():
    """Main ETL flow to load data into Big Query"""
    color = "yellow"
    year = 2021
    month = 1

    path = extract_from_gcs(color, year, month)
    df = transform(path)
    write_bq(df)


if __name__ == "__main__":
    etl_gcs_to_bq()

# In the terminal run:
# cd week_2_workflow_orchestration\prefect-zoomcamp\flows\02_gcp
# python etl_gcs_to_bq.py