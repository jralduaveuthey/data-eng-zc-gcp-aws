import boto3
import json
import os
from pathlib import Path
from botocore.exceptions import ClientError
import time


def start_cluster(cluster_id):
    emr = boto3.client('emr')
    response = emr.set_termination_protection(
        JobFlowIds=[cluster_id],
        TerminationProtected=True
    )
    print(f"Started the EMR cluster with ID: {cluster_id}; with response {response}")
    return response

def upload_file_to_s3(bucket, local_file, s3_key):
    s3 = boto3.client('s3')
    try:
        s3.upload_file(local_file, bucket, s3_key)
        print(f"File {local_file} uploaded to {bucket}/{s3_key}")
    except ClientError as e:
        print(e)
        return False
    return True

def execute_pyspark_script(cluster_id, bucket, pyspark_script_s3_key, input_green, input_yellow, output):
    emr = boto3.client('emr')
    response = emr.add_job_flow_steps(
        JobFlowId=cluster_id,
        Steps=[
            {
                'Name': 'Execute PySpark Script',
                'ActionOnFailure': 'CONTINUE',
                'HadoopJarStep': {
                    'Jar': 'command-runner.jar',
                    'Args': [
                        'spark-submit',
                        '--deploy-mode', 'cluster',
                        f's3://{bucket}/{pyspark_script_s3_key}',
                        '--input_green', input_green,
                        '--input_yellow', input_yellow,
                        '--output', output
                    ]
                }
            }
        ]
    )
    print(f"Started the PySpark script with step ID: {response['StepIds'][0]}")
    return response['StepIds'][0]


def wait_for_cluster_ready(cluster_id):
    emr = boto3.client('emr')
    while True:
        response = emr.describe_cluster(ClusterId=cluster_id)
        status = response['Cluster']['Status']['State']
        if status in ['RUNNING', 'WAITING']:
            print(f"Cluster {cluster_id} is ready with status: {status}")
            break
        elif status in ['TERMINATING', 'TERMINATED', 'TERMINATED_WITH_ERRORS']:
            raise Exception(f"Cluster {cluster_id} is in an invalid state: {status}")
        time.sleep(30)


def wait_for_step_completion(cluster_id, step_id):
    emr = boto3.client('emr')
    while True:
        response = emr.describe_step(ClusterId=cluster_id, StepId=step_id)
        status = response['Step']['Status']['State']
        if status in ['COMPLETED', 'FAILED', 'CANCELLED', 'INTERRUPTED']:
            print(f"Step {step_id} finished with status: {status}")
            break
        time.sleep(30)

def main():
    with open(Path(os.path.abspath('emr_config.json')), 'r') as f:
        config = json.load(f)

    cluster_id = config['cluster_id']

    # Start the cluster
    start_cluster(cluster_id)
    
    # Upload the file to S3
    local_file = '06_spark_sql.py'
    s3_bucket = config['s3_bucket']
    s3_key = 'code/06_spark_sql.py'
    upload_file_to_s3(s3_bucket, local_file, s3_key)

    # Execute the PySpark script
    pyspark_script_s3_key = 'code/06_spark_sql.py'
    input_green = f's3://{s3_bucket}/pq/green/2021/*/*/'
    input_yellow = f's3://{s3_bucket}/pq/yellow/2021/*/*/'
    output = f's3://{s3_bucket}/report-2021'
    step_id = execute_pyspark_script(cluster_id, s3_bucket, pyspark_script_s3_key, input_green, input_yellow, output)

    # Wait for the step to complete
    wait_for_step_completion(cluster_id, step_id)



if __name__ == '__main__':
    main()