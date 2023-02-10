from pathlib import Path
import pandas as pd
from prefect import flow, task, get_run_logger
from prefect.tasks import task_input_hash
from datetime import timedelta
from prefect_aws.s3 import S3Bucket # see https://prefecthq.github.io/prefect-aws/s3/
import configparser  # To read the config file
import boto3
from subprocess import check_output
import time


v_config = configparser.ConfigParser()
config_file = Path(__file__).parent / "config.cfg"
v_config.read(config_file)
v_redshift_config = v_config["redshift-config"]

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
def write_s3(path: Path) -> None:
    """Upload local parquet file to S3"""
    # you need to create the bucket in AWS => called "zoom-s3-jrv"
    # and the blocks in Prefect UI for AWS Credentials and S3 Bucket. Important to choose the block type "S3 Bucket" and not "S3" !!!
    s3_block = S3Bucket.load("zoom-s3-jrv")
    s3_block.upload_from_path(from_path=path, to_path=path.parent.parent.name + '/' + path.parent.name + '/' + path.name)
    return

@task()
def check_exists_s3(s3_path: str) -> bool:
    """Check if the file already exists in S3"""
    #TODO: check how (if possible) to implement this function using the prefect block
    s3 = boto3.client('s3')
    bucket_name = "zoom-s3-jrv"
    try:
        s3.head_object(Bucket=bucket_name, Key=s3_path)
        exists = True
    except Exception as e:
        exists = False
    return exists




@task(retries=3)
def extract_from_s3(color: str, year: int, month: int, s3_path: str) -> Path:
    """Download trip data from S3"""
    s3_block = S3Bucket.load("zoom-s3-jrv")
    localpath = Path(__file__).parent 
    localpath= localpath.parent.parent.name + '/' + localpath.parent.name + '/' + localpath.name + f"/data/{color}/{color}_tripdata_{year}-{month:02}.parquet"
    # if the file exists locally then we can skip the download
    if not Path(localpath).exists():
        s3_block.download_object_to_path(from_path=s3_path, to_path=localpath)
    else:
        print(f"File {localpath} already exists locally. Skipping download in extract_from_s3()...")
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

def map_df_dtypes_to_sql(dtype):
    if dtype == 'float64':
        return 'FLOAT'
    elif dtype == 'int64':
        return 'BIGINT' #'INTEGER'
    elif dtype == 'object':
        return 'VARCHAR(50)'
    elif dtype == 'datetime64[ns]':
        return 'TIMESTAMP'
    else:
        return 'VARCHAR(50)'



@task()
def write_redshift(df: pd.DataFrame, year: int, month: int, color: str) -> None:
    """Write DataFrame to BiqQuery"""
    logger = get_run_logger()
    #TODO NOW: implement this function for redshift
    cluster_redshift = v_redshift_config["cluster_redshift"]
    user_redshift = v_redshift_config["user_redshift"]
    db_redshift = v_redshift_config["db_redshift"]
    arn_redshift = v_redshift_config["arn_redshift"]
    table = f"{db_redshift}.trip_data_all.{color}_tripdata"

    # Settings for the Redshift Data API 
    region = 'eu-central-1'
    user = user_redshift
    cluster = cluster_redshift
    database = db_redshift

    # Get a schema for the table from the DataFrame df
    # UNCOMMENT THIS TO CREATE THE TABLE
    # logger.info('Creating the table...')
    # df_dtypes = df.dtypes
    # sql_dtypes = df_dtypes.apply(map_df_dtypes_to_sql).to_dict()
    # sql_query = f"CREATE TABLE {table} ({', '.join([f'{k} {v}' for k, v in sql_dtypes.items()])});"
    # terminal_cmd = f'aws redshift-data execute-statement --region {region} --db-user {user} --cluster-identifier {cluster} --database {database} --sql "{sql_query}"'
    # check_output(terminal_cmd) # this creates the table and can be seen in the redshift console using the query editor v2

    # Enter data into the table
    #NOTE: if you just want to enter a few rows see week_2_workflow_orchestration\prefect-zoomcamp\flows\02_aws\etl_s3_to_redshift.py

    logger.info('Inserting data into the table...')
    start_time = time.time()
    sql_query = f"COPY {table} FROM 's3://zoom-s3-jrv/data/{color}/{color}_tripdata_{year}-{month:02}.parquet' IAM_ROLE '{arn_redshift}' FORMAT AS PARQUET;"
    #TODO(nice to have): add a "zoom-s3-jrv" as a variable across the whole script
    terminal_cmd = f'aws redshift-data execute-statement --region {region} --db-user {user} --cluster-identifier {cluster} --database {database} --sql "{sql_query}"'
    check_output(terminal_cmd)
    time_taken = str(time.time() - start_time)
    logger.info(f"Time to insert {color}_tripdata_{year}-{month:02}.parquet from S3 into the table: {time_taken} seconds")

    return



@flow()
def etl_web_to_s3(year: int, month: int, color: str) -> None:
    """The main ETL function"""
    dataset_file = f"{color}_tripdata_{year}-{month:02}"
    dataset_url = f"https://github.com/DataTalksClub/nyc-tlc-data/releases/download/{color}/{dataset_file}.csv.gz"

    s3_path = f"data/{color}/{dataset_file}.parquet" 
    if check_exists_s3(s3_path):
        print(f"File {s3_path} already exists in S3. Skipping...")
        return

    df = fetch(dataset_url)
    df_clean = clean(df)
    path = write_local(df_clean, color, dataset_file)
    write_s3(path)


@flow()
def etl_s3_to_refshift(year: int, month: int, color: str) -> None:
    """Main ETL flow to load data into Big Query"""
    s3_path = f"data/{color}/{color}_tripdata_{year}-{month:02}.parquet"
    if check_exists_s3(s3_path):
        path = extract_from_s3(color, year, month, s3_path)
    else:
        raise FileNotFoundError(f"Error in extract_from_s3(): File {s3_path} does not exist in S3")
    df = transform(path)
    write_redshift(df, year, month, color)


@flow()
def etl_parent_flow(
    months: list[int] = [1, 2], year: int = 2021, color: str = "yellow"
):
    logger = get_run_logger()
    # for month in months: #TODO: uncomment this if you want to run the web_to_s3 flow
    #     etl_web_to_s3(year, month, color)
    #     print(f"Finished 'etl_web_to_s3' for  {color} + {year} + {month}")
    #     logger.info(f"Finished 'etl_web_to_s3' for  {color} + {year} + {month}")
    for month in months:
        etl_s3_to_refshift(year, month, color)
        print(f"Finished 'etl_s3_to_refshift' for  {color} + {year} + {month}")
        logger.info(f"Finished 'etl_s3_to_refshift' for  {color} + {year} + {month}")

    

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
# python web_to_s3_to_redshift.py
