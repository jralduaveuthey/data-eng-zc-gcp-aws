from pathlib import Path
import pandas as pd
from prefect import flow, task, get_run_logger
from prefect_gcp.cloud_storage import GcsBucket
from random import randint
from prefect.tasks import task_input_hash
from datetime import timedelta
from google.cloud import storage
from prefect_gcp import GcpCredentials


@task(retries=3,cache_key_fn=task_input_hash, cache_expiration=timedelta(days=1))
def fetch(dataset_url: str) -> pd.DataFrame:
    """Read taxi data from web into pandas DataFrame"""
    # if randint(0, 1) > 0:
    #     raise Exception

    df = pd.read_csv(dataset_url)
    return df


@task(log_prints=True)
def clean(df=pd.DataFrame) -> pd.DataFrame:
    """Fix dtype issues"""
    # NOTE: add something here if necessary...atm not needed
    # df["tpep_pickup_datetime"] = pd.to_datetime(df["tpep_pickup_datetime"])
    # print(df.head(2))
    # print(f"columns: {df.dtypes}")
    # print(f"rows: {len(df)}")
    return df


@task()
def write_local(df: pd.DataFrame, color: str, dataset_file: str) -> Path:
    """Write DataFrame out locally as parquet file"""
    # use "Path" to create a path object under the directory of this file
    path = Path(__file__).parent / f"data/{color}/{dataset_file}.parquet"
    # create the directory if it doesn't exist
    path.parent.mkdir(parents=True, exist_ok=True)
    df.to_parquet(path, compression="gzip")
    return path


@task()
def write_gcs(path: Path) -> None:
    """Upload local parquet file to GCS"""
    gcs_block = GcsBucket.load("zoom-gcs")
    gcs_block.upload_from_path(from_path=path, to_path=path.parent.parent.name + '/' + path.parent.name + '/' + path.name)
    return

@task()
def check_exists_gcs(gcs_path: str) -> bool:
    """Check if the file already exists in GCS"""
    #TODO: check how (if possible) to implement this function using the prefect block
    client = storage.Client()
    bucket = client.bucket("zoom-gcs")
    blob = bucket.blob(gcs_path)
    exists = blob.exists()
    return exists




@task(retries=3)
def extract_from_gcs(color: str, year: int, month: int, gcs_path: str) -> Path:
    """Download trip data from GCS"""
    gcs_block = GcsBucket.load("zoom-gcs")
    localpath = Path(__file__).parent 
    localpath= localpath.parent.name + '/' + localpath.name + f"/data/{color}/{color}_tripdata_{year}-{month:02}.parquet"
    # if the file exists locally then we can skip the download
    if not Path(localpath).exists():
        gcs_block.download_object_to_path(from_path=gcs_path, to_path=localpath)
    else:
        print(f"File {localpath} already exists locally. Skipping download in extract_from_gcs()...")
    return localpath 


@task()
def transform(path: Path) -> pd.DataFrame:
    """Data cleaning example"""
    df = pd.read_parquet(path)
    # NOTE: atm this is not needed
    # print(f"pre: missing passenger count: {df['passenger_count'].isna().sum()}")
    # df["passenger_count"].fillna(0, inplace=True)
    # print(f"post: missing passenger count: {df['passenger_count'].isna().sum()}")
    return df


@task()
def write_bq(df: pd.DataFrame, color: str) -> None:
    """Write DataFrame to BiqQuery"""

    gcp_credentials_block = GcpCredentials.load("zoom-gcp-creds")
    
    if color == "yellow":
        dest_table_name = "trips_data_all.yellow_tripdata"
    elif color == "green":
        dest_table_name = "trips_data_all.green_tripdata"
    elif color == "fhv":
        dest_table_name = "trips_data_all.fhv_tripdata"
    else:
        raise ValueError("color must be one of 'yellow', 'green', 'fhv'")

    df.to_gbq(
        destination_table=dest_table_name,
        project_id="dtc-de-375211",
        credentials=gcp_credentials_block.get_credentials_from_service_account(),
        chunksize=500_000,
        if_exists="append",
    )
    return



@flow()
def etl_web_to_gcs(year: int, month: int, color: str) -> None:
    """The main ETL function"""
    dataset_file = f"{color}_tripdata_{year}-{month:02}"
    dataset_url = f"https://github.com/DataTalksClub/nyc-tlc-data/releases/download/{color}/{dataset_file}.csv.gz"

    gcs_path = f"data/{color}/{dataset_file}.parquet"
    if check_exists_gcs(gcs_path):
        print(f"File {gcs_path} already exists in GCS. Skipping...")
        return

    df = fetch(dataset_url)
    df_clean = clean(df)
    path = write_local(df_clean, color, dataset_file)
    write_gcs(path)

@flow()
def etl_gcs_to_bq(year: int, month: int, color: str) -> None:
    """Main ETL flow to load data into Big Query"""
    gcs_path = f"data/{color}/{color}_tripdata_{year}-{month:02}.parquet"
    if check_exists_gcs(gcs_path):
        path = extract_from_gcs(color, year, month, gcs_path)
    else:
        raise FileNotFoundError(f"Error in extract_from_gcs(): File {gcs_path} does not exist in GCS")
    df = transform(path)
    write_bq(df, color)


@flow()
def etl_parent_flow(
    months: list[int] = [1, 2], year: int = 2021, color: str = "yellow"
):
    logger = get_run_logger()
    #TODO: uncomment this if you also need the web_to_gcs flow
    # for month in months:
    #     etl_web_to_gcs(year, month, color)
    #     print(f"Finished 'etl_web_to_gcs' for  {color} + {year} + {month}")
    #     logger.info(f"Finished 'etl_web_to_gcs' for  {color} + {year} + {month}")
    for month in months:
        etl_gcs_to_bq(year, month, color)
        print(f"Finished 'etl_gcs_to_bq' for  {color} + {year} + {month}")
        logger.info(f"Finished 'etl_gcs_to_bq' for  {color} + {year} + {month}")

    

if __name__ == "__main__":
    color = "yellow"
    months = [i for i in range(1, 13)]
    year = 2019
    etl_parent_flow(months, year, color)
    print(f"Finished 'etl_parent_flow' for {color} + {year}")

    color = "yellow"
    months = [i for i in range(1, 13)]
    year = 2020
    etl_parent_flow(months, year, color)
    print(f"Finished 'etl_parent_flow' for {color} + {year}")

    color = "green"
    months = [i for i in range(1, 13)]
    year = 2019
    etl_parent_flow(months, year, color)
    print(f"Finished 'etl_parent_flow' for {color} + {year}")

    color = "green"
    months = [i for i in range(1, 13)]
    year = 2020
    etl_parent_flow(months, year, color)
    print(f"Finished 'etl_parent_flow' for {color} + {year}")

    # color = "fhv"
    # months = [i for i in range(1, 13)]
    # year = 2019
    # etl_parent_flow(months, year, color)


# Run the flow
# python web_to_gcs_to_bq.py
