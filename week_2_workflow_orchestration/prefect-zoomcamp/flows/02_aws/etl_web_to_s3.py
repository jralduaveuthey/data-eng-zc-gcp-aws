from pathlib import Path
import pandas as pd
from prefect import flow, task
from random import randint
from prefect_aws.s3 import S3Bucket # see https://prefecthq.github.io/prefect-aws/s3/



@task(retries=3)
def fetch(dataset_url: str) -> pd.DataFrame:
    """Read taxi data from web into pandas DataFrame"""
    # if randint(0, 1) > 0:
    #     raise Exception

    df = pd.read_csv(dataset_url)
    return df


@task(log_prints=True)
def clean(df=pd.DataFrame) -> pd.DataFrame:
    """Fix dtype issues"""
    df["tpep_pickup_datetime"] = pd.to_datetime(df["tpep_pickup_datetime"])
    print(df.head(2))
    print(f"columns: {df.dtypes}")
    print(f"rows: {len(df)}")
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
def write_s3(path: Path) -> None:
    """Upload local parquet file to S3"""
    # you need to create the bucket in AWS => called "zoom-s3-jrv"
    # and the blocks in Prefect UI for AWS Credentials and S3 Bucket. Important to choose the block type "S3 Bucket" and not "S3" !!!
    s3_block = S3Bucket.load("zoom-s3-jrv")
    s3_block.upload_from_path(from_path=path, to_path=path.parent.parent.name + '/' + path.parent.name + '/' + path.name)

    return


@flow()
def etl_web_to_s3() -> None:
    """The main ETL function"""
    color = "yellow"
    year = 2021
    month = 1
    dataset_file = f"{color}_tripdata_{year}-{month:02}"
    dataset_url = f"https://github.com/DataTalksClub/nyc-tlc-data/releases/download/{color}/{dataset_file}.csv.gz"

    df = fetch(dataset_url)
    df_clean = clean(df)
    path = write_local(df_clean, color, dataset_file)
    write_s3(path)


if __name__ == "__main__":
    etl_web_to_s3()

# In the terminal run:
# cd week_2_workflow_orchestration\prefect-zoomcamp\flows\02_aws
# python etl_web_to_s3.py