from pathlib import Path
import pandas as pd
from prefect import flow, task
from prefect_aws.s3 import S3Bucket # see https://prefecthq.github.io/prefect-aws/s3/
from prefect_aws import AwsCredentials
import pandas_redshift as pr
from sqlalchemy import create_engine
from sqlalchemy import text
from subprocess import check_output
import json



@task(retries=3)
def extract_from_s3(color: str, year: int, month: int) -> Path:
    """Download trip data from GCS"""
    s3_path = "data"
    s3_block = S3Bucket.load("zoom-s3-jrv")
    localpath = Path(__file__).parent / "data"
    s3_block.get_directory(from_path=s3_path, local_path=localpath)
    # to download a single file, use s3_block.download_object_to_path # see https://prefecthq.github.io/prefect-aws/s3/#prefect_aws.s3.S3Bucket.download_object_to_path
    return localpath / f"{color}/{color}_tripdata_{year}-{month:02}.parquet"

@task()
def transform(path: Path) -> pd.DataFrame:
    """Data cleaning example"""
    df = pd.read_parquet(path)
    print(f"pre: missing passenger count: {df['passenger_count'].isna().sum()}")
    df["passenger_count"].fillna(0, inplace=True)
    print(f"post: missing passenger count: {df['passenger_count'].isna().sum()}")
    return df


@task()
def write_redshift(df: pd.DataFrame) -> None:
    """Write DataFrame to Redshift"""
    # TODO: XAKI trying to connect to redshift

    aws_credentials_block = AwsCredentials.load("aws-seads-jaime-dev")

    # # ATTEMPT 1: see https://github.com/MyBusinessMaterialLinks/Youtube/blob/main/Connecting%20to%20AWS%20Redshift%20via%20Jupyter/Connecting%20to%20AWS%20Redshift.ipynb
    # # Does not work because Redshift in a VPC with a Security Group that does not allow inbound traffic from the internet
    # redshift_endpoint1 = "trips-data-all.cmaxzujjaz2k.eu-central-1.redshift.amazonaws.com:5439/"
    # redshift_user1 = "exampleuser"
    # redshift_pass1 = "..."
    # port1 = 8192 #whaterver your Redshift portnumber is
    # dbname1 = "dev" #the default Redshift database name is "dev"
    # engine_string = "postgresql+psycopg2://%s:%s@%s:%d/%s" \
    # % (redshift_user1, redshift_pass1, redshift_endpoint1, port1, dbname1)
    # engine1 = create_engine(engine_string)
    # df.to_sql('your_table', engine1, index=False, if_exists='replace')

    # ATTEMPT 2: see https://stackoverflow.com/questions/38402995/how-to-write-data-to-redshift-that-is-a-result-of-a-dataframe-created-in-python
    # Does not work because Redshift in a VPC with a Security Group that does not allow inbound traffic from the internet
    # conn = create_engine('postgresql://exampleuser:Mustbe8characters@trips-data-all.cmaxzujjaz2k.eu-central-1.redshift.amazonaws.com:5439/dev')
    # df2 = pd.DataFrame([{'A': 'foo', 'B': 'green', 'C': 11},{'A':'bar', 'B':'blue', 'C': 20}])
    # df2.to_sql('your_table', conn, index=False, if_exists='replace')

    # # ATTEMPT 3: see https://github.com/agawronski/pandas_redshift 
    # # Does not work because Redshift in a VPC with a Security Group that does not allow inbound traffic from the internet
    # pr.connect_to_redshift(dbname = 'dev',
    #                         host = 'trips-data-all.cmaxzujjaz2k.eu-central-1.redshift.amazonaws.com',
    #                         port = 5439,
    #                         user = "exampleuser",
    #                         password = "Mustbe8characters")
    # pr.connect_to_s3(aws_access_key_id = aws_credentials_block.aws_access_key_id,
    #                 aws_secret_access_key = aws_credentials_block.aws_secret_access_key,
    #                 bucket = "zoom-s3-jrv")
    #                 # subdirectory = <subdirectory>)
    # # Write the DataFrame to S3 and then to redshift
    # pr.pandas_to_redshift(data_frame = df,
    #                         redshift_table_name = 'gawronski.nba_shots_log')







    # # ATTEMPT 4: using Redshift Data API. See https://stackoverflow.com/questions/64782710/redshift-odbc-driver-test-fails-is-the-server-running-on-host-and-accepting-tcp 
    # # and https://docs.aws.amazon.com/redshift/latest/mgmt/data-api-calling.html

    # SQL command to create a table called "your_table" in the "dev" database in redshift
    # CREATE TABLE dev.public.my_tablee_from_redshift (
    #     vendor_id VARCHAR(50),
    #     pickup_datetime TIMESTAMP,
    #     dropoff_datetime TIMESTAMP,
    #     passenger_count INTEGER,
    #     trip_distance FLOAT,
    #     pickup_longitude FLOAT,
    #     pickup_latitude FLOAT,
    #     rate_code INTEGER,
    #     store_and_fwd_flag VARCHAR(1),
    #     dropoff_longitude FLOAT,
    #     dropoff_latitude FLOAT,
    #     payment_type INTEGER,
    #     fare_amount FLOAT,
    #     extra FLOAT,
    #     mta_tax FLOAT,
    #     tip_amount FLOAT,
    #     tolls_amount FLOAT,
    #     improvement_surcharge FLOAT,
    #     total_amount FLOAT,
    #     congestion_surcharge FLOAT
    # );


    # Settings for the Redshift Data API 
    region = 'eu-central-1'
    user = 'exampleuser'
    cluster = 'trips-data-all'
    database = 'dev'

    # This block of code works
    sql_query = 'SELECT table_schema,table_name FROM information_schema.tables;' # this returns All the tables
    terminal_cmd = f'aws redshift-data execute-statement --region {region} --db-user {user} --cluster-identifier {cluster} --database {database} --sql "{sql_query}"'
    out = check_output(terminal_cmd)
    out_text = out.decode("utf-8") # get the text from the terminal output
    out_dict = json.loads(out_text)# convert the text to a dictionary
    statement_id = out_dict['Id']# get the statement ID from the dictionary
    sql_query2 = f'aws redshift-data get-statement-result --id {statement_id}'
    out2 = check_output(sql_query2)
    out2_text = out2.decode("utf-8") # get the text from the terminal output
    out2_dict = json.loads(out2_text)# convert the text to a dictionary
    out2_dict['Records'] # get the records from the dictionary


    # Get a schema for the table from the DataFrame df
    # schema = df.dtypes.apply(lambda x: x.name).to_dict()
    # sql_query = f"CREATE TABLE dev.public.table_from_python ({', '.join([f'{k} {v}' for k, v in schema.items()])});"
    sql_query = "CREATE TABLE public.my_table_from_python1 (vendor_id VARCHAR(50),  pickup_datetime TIMESTAMP);"
    terminal_cmd = f'aws redshift-data execute-statement --region {region} --db-user {user} --cluster-identifier {cluster} --database {database} --sql "{sql_query}"'
    out = check_output(terminal_cmd) # TODO: XAKI..this is getting executed but I see no new table in redshift
                                    #...if this does not work move to ATTEMPT 5 and if not then create the cluster from scratch as in ATTEMPT 6
    out_text = out.decode("utf-8")
    out_dict = json.loads(out_text)
    statement_id = out_dict['Id']
    sql_query2 = f'aws redshift-data get-statement-result --id {statement_id}'
    out2 = check_output(sql_query2)
    out2_text = out2.decode("utf-8") 
    out2_dict = json.loads(out2_text) 

    sql_query = "SELECT * FROM dev.public.my_table_from_redshift;"
    terminal_cmd = f'aws redshift-data execute-statement --region {region} --db-user {user} --cluster-identifier {cluster} --database {database} --sql "{sql_query}"'
    out = check_output(terminal_cmd)
    out_text = out.decode("utf-8")


    # # ATTEMPT 5: use this aws python connector to connect to an existing cluster: https://docs.aws.amazon.com/redshift/latest/mgmt/python-connect-examples.html

    # # ATTEMPT 6: use boto3 also to create the cluster https://tsakunelsonz.medium.com/creating-a-redshift-cluster-using-with-aws-python-sdk-9ba51416473

@flow()
def etl_gcs_to_bq():
    """Main ETL flow to load data into Big Query"""
    color = "yellow"
    year = 2021
    month = 1

    path = extract_from_s3(color, year, month)
    df = transform(path)
    write_redshift(df)


if __name__ == "__main__":
    etl_gcs_to_bq()
