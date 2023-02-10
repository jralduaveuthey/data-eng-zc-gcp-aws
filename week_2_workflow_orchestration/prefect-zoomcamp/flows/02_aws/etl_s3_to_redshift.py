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
import configparser  # To read the config file

v_config = configparser.ConfigParser()
config_file = Path(__file__).parent / "config.cfg"
v_config.read(config_file)
v_redshift_config = v_config["redshift-config"]

@task(retries=3)
def extract_from_s3(color: str, year: int, month: int) -> Path:
    """Download trip data from S3"""
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

def map_df_dtypes_to_sql(dtype):
    if dtype == 'float64':
        return 'FLOAT'
    elif dtype == 'int64':
        return 'BIGINT'
    elif dtype == 'object':
        return 'VARCHAR(50)'
    elif dtype == 'datetime64[ns]':
        return 'TIMESTAMP'
    else:
        return 'VARCHAR(50)'

@task()
def write_redshift(df: pd.DataFrame) -> None:
    """Write DataFrame to Redshift"""

    # aws_credentials_block = AwsCredentials.load("aws-seads-jaime-dev")

    cluster_redshift = v_redshift_config["cluster_redshift"]
    user_redshift = v_redshift_config["user_redshift"]
    db_redshift = v_redshift_config["db_redshift"]
    password_redshift = v_redshift_config["password_redshift"]

    # # ATTEMPT 1: see https://github.com/MyBusinessMaterialLinks/Youtube/blob/main/Connecting%20to%20AWS%20Redshift%20via%20Jupyter/Connecting%20to%20AWS%20Redshift.ipynb
    # # Does not work because Redshift in a VPC with a Security Group that does not allow inbound traffic from the internet
    # redshift_endpoint1 = "trips-data-all.cmaxzujjaz2k.eu-central-1.redshift.amazonaws.com:5439/"
    # redshift_user1 = user_redshift
    # redshift_pass1 = password_redshift
    # port1 = 8192 #whaterver your Redshift portnumber is
    # dbname1 = "dev" #the default Redshift database name is "dev"
    # engine_string = "postgresql+psycopg2://%s:%s@%s:%d/%s" \
    # % (redshift_user1, redshift_pass1, redshift_endpoint1, port1, dbname1)
    # engine1 = create_engine(engine_string)
    # df.to_sql('your_table', engine1, index=False, if_exists='replace')

    # ATTEMPT 2: see https://stackoverflow.com/questions/38402995/how-to-write-data-to-redshift-that-is-a-result-of-a-dataframe-created-in-python
    # Does not work because Redshift in a VPC with a Security Group that does not allow inbound traffic from the internet
    # conn = create_engine('postgresql://USER:PSW@XXX.redshift.amazonaws.com:5439/dev')
    # df2 = pd.DataFrame([{'A': 'foo', 'B': 'green', 'C': 11},{'A':'bar', 'B':'blue', 'C': 20}])
    # df2.to_sql('your_table', conn, index=False, if_exists='replace')

    # # ATTEMPT 3: see https://github.com/agawronski/pandas_redshift 
    # # Does not work because Redshift in a VPC with a Security Group that does not allow inbound traffic from the internet
    # pr.connect_to_redshift(dbname = 'dev',
    #                         host = 'trips-data-all.cmaxzujjaz2k.eu-central-1.redshift.amazonaws.com',
    #                         port = 5439,
    #                         user = user_redshift ,
    #                         password = password_redshift )
    # pr.connect_to_s3(aws_access_key_id = aws_credentials_block.aws_access_key_id,
    #                 aws_secret_access_key = aws_credentials_block.aws_secret_access_key,
    #                 bucket = "zoom-s3-jrv")
    #                 # subdirectory = <subdirectory>)
    # # Write the DataFrame to S3 and then to redshift
    # pr.pandas_to_redshift(data_frame = df,
    #                         redshift_table_name = 'gawronski.nba_shots_log')




    # # ATTEMPT 4: using Redshift Data API. See https://stackoverflow.com/questions/64782710/redshift-odbc-driver-test-fails-is-the-server-running-on-host-and-accepting-tcp 
    # # and https://docs.aws.amazon.com/redshift/latest/mgmt/data-api-calling.html

    # Settings for the Redshift Data API 
    region = 'eu-central-1'
    user = user_redshift
    cluster = cluster_redshift
    database = db_redshift

    # Get a schema for the table from the DataFrame df
    # UNCOMMENT THIS TO CREATE THE TABLE
    print('Creating the table...')
    df_dtypes = df.dtypes
    sql_dtypes = df_dtypes.apply(map_df_dtypes_to_sql).to_dict()
    sql_query = f"CREATE TABLE dev.public.my_table_from_python ({', '.join([f'{k} {v}' for k, v in sql_dtypes.items()])});"
    terminal_cmd = f'aws redshift-data execute-statement --region {region} --db-user {user} --cluster-identifier {cluster} --database {database} --sql "{sql_query}"'
    check_output(terminal_cmd) # this creates the table and can be seen in the redshift console using the query editor v2

    # Enter data into the table
    print('Inserting data into the table...')
    # sql_query = "INSERT INTO dev.public.my_table_from_python (vendor_id, pickup_datetime) VALUES ('dummy_vendor', '2021-01-01 00:00:00');" #dummy to test
    # create a list of tuples with the values to insert
    values = df.values.tolist()
    # use only a few rows since I am just testing
    values = values[:10]
    values = [tuple(x) for x in values]
    # create a list of strings with the values to insert
    values_str = [str(x) for x in values]
    # create a string with the values to insert
    values_str = ', '.join(values_str)
    # every time there is "Timestamp" in the string, delete it leaving only the string between the brackets
    values_str = values_str.replace("Timestamp('", "'")
    values_str = values_str.replace("')", "'")
    # create the sql query
    sql_query = f"INSERT INTO dev.public.my_table_from_python VALUES {values_str};"
    terminal_cmd = f'aws redshift-data execute-statement --region {region} --db-user {user} --cluster-identifier {cluster} --database {database} --sql "{sql_query}"'
    check_output(terminal_cmd)

    # Read the data from the table
    print('Reading data from the table...')
    sql_query = "SELECT * FROM dev.public.my_table_from_python LIMIT 15;" #dummy to test
    terminal_cmd = f'aws redshift-data execute-statement --region {region} --db-user {user} --cluster-identifier {cluster} --database {database} --sql "{sql_query}"'
    out = check_output(terminal_cmd)
    out_text = out.decode("utf-8")
    out_dict = json.loads(out_text)
    statement_id = out_dict['Id']
    sql_query2 = f'aws redshift-data get-statement-result --id {statement_id}'
    out2 = check_output(sql_query2)
    out2_text = out2.decode("utf-8") 
    out2_dict = json.loads(out2_text) 
    out2_records = out2_dict['Records'] # get the records from the dictionary

    records_list = []
    for record in out2_records:
        output_dict = {}
        i = 0
        for item in record:
            for key, value in item.items():
                key = out2_dict['ColumnMetadata'][i]['label']
                
                if key not in output_dict:
                    output_dict[key] = value
                    i += 1
                else:
                    # raise an error cause this should never happen since all the columns in the db are unique
                    raise ValueError(f"Key {key} already exists in the dictionary")

        records_list.append(output_dict)

    for record in records_list:# print the values in the records
        print("The content of the row number {} is:".format(records_list.index(record)))
        print(record)


    # # ATTEMPT 5: use this aws python connector to connect to an existing cluster: https://docs.aws.amazon.com/redshift/latest/mgmt/python-connect-examples.html
    # Not tried since ATTEMPT 4 works

    # # ATTEMPT 6: use boto3 also to create the cluster https://tsakunelsonz.medium.com/creating-a-redshift-cluster-using-with-aws-python-sdk-9ba51416473
    # Not tried since ATTEMPT 4 works

@flow()
def etl_s3_to_refshift():
    """Main ETL flow to load data into Redshift"""
    color = "yellow"
    year = 2021
    month = 1

    path = extract_from_s3(color, year, month)
    df = transform(path)
    write_redshift(df)


if __name__ == "__main__":
    etl_s3_to_refshift()
