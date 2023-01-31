from pathlib import Path
import pandas as pd
from prefect import flow, task
from prefect_gcp.cloud_storage import GcsBucket
from prefect_gcp import GcpCredentials
import sys


@task(retries=3)
def extract_from_gcs(color: str, year: int, month: int) -> Path:
    """Download trip data from GCS"""
    gcs_path = "data"
    gcs_block = GcsBucket.load("zoom-gcs")
    localpath = Path(__file__).parent 
    gcs_block.get_directory(from_path=gcs_path, local_path=localpath)
     # to download a single file, use s3_block. # see https://prefecthq.github.io/prefect-gcp/
    return localpath / f"data/{color}/{color}_tripdata_{year}-{month:02}.parquet"


@task()
def transform(path: Path) -> pd.DataFrame:
    """Data cleaning example"""
    df = pd.read_parquet(path)
    # print(f"pre: missing passenger count: {df['passenger_count'].isna().sum()}")
    # df["passenger_count"].fillna(0, inplace=True)
    # print(f"post: missing passenger count: {df['passenger_count'].isna().sum()}")
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
# in the definition of the function add default values for the input arguments
def etl_gcs_to_bq(months: list, year: int, color: str) -> None:
    """Main ETL flow to load data into Big Query"""
    for month in months:
        path = extract_from_gcs(color, year, month)
        df = transform(path)
        write_bq(df)


if __name__ == "__main__":
    months = sys.argv[1]  # [2,3]
    year = sys.argv[2] # 2019
    color = sys.argv[3] # "yellow"
    etl_gcs_to_bq(months, year, color)

# In the terminal run:
# cd week_2_workflow_orchestration\prefect-zoomcamp\flows\homework
# prefect deployment build ./etl_gcs_to_bq.py:etl_gcs_to_bq --name homeworkflow --param months=[2,3] --param year=2019 --param color="yellow"
# prefect deployment apply etl_gcs_to_bq-deployment.yaml
# prefect agent start -q 'default'
# run the flow from the UI