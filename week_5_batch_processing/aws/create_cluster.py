import boto3
import json
import os
from pathlib import Path

def create_cluster(config):
    emr = boto3.client('emr')
    
    cluster = emr.run_job_flow(
        Name=config['cluster_name'],
        ReleaseLabel='emr-5.36.0',
        Instances={
            'InstanceGroups': [
                {
                    'Name': 'Master nodes',
                    'InstanceRole': 'MASTER',
                    'InstanceType': config['instance_type'],
                    'InstanceCount': 1,
                },
                {
                    'Name': 'Worker nodes',
                    'InstanceRole': 'CORE',
                    'InstanceType': config['instance_type'],
                    'InstanceCount': config['instance_count'] - 1,
                }
            ],
            'Ec2KeyName': config['key_pair'],
            'KeepJobFlowAliveWhenNoSteps': True
        },
        Applications=[
            {
                'Name': 'Spark',
            },
            {
                'Name': 'Zeppelin',
            }
        ],
        JobFlowRole='EMR_EC2_DefaultRole',
        ServiceRole='EMR_DefaultRole',
        VisibleToAllUsers=True,
        LogUri=f's3://{config["s3_bucket"]}/logs/',
        AutoTerminationPolicy={
            'IdleTimeout': 1800  # Set the idle timeout in seconds
        },
    )
    print(f"Created the EMR cluster with ID: {cluster['JobFlowId']}")
    return cluster['JobFlowId']

def update_emr_config(config, cluster_id):
    config['cluster_id'] = cluster_id
    with open(Path(os.path.abspath('emr_config.json')), 'w') as f:
        json.dump(config, f, indent=4)
    print(f"Updated emr_config.json with the cluster ID: {cluster_id}")

def main():
    with open(Path(os.path.abspath('emr_config.json')), 'r') as f:
        config = json.load(f)

    # Create the cluster
    cluster_id = create_cluster(config)

    # Update the emr_config.json file with the cluster_id
    update_emr_config(config, cluster_id)
    

if __name__ == '__main__':
    main()
