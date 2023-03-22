import boto3
import json
import os
from pathlib import Path

def terminate_cluster(cluster_id):
    emr = boto3.client('emr')
    
    # Disable termination protection
    emr.set_termination_protection(
        JobFlowIds=[cluster_id],
        TerminationProtected=False
    )
    
    # Terminate the cluster
    emr.terminate_job_flows(JobFlowIds=[cluster_id])

def main():
    with open(Path(os.path.abspath('emr_config.json')), 'r') as f:
        config = json.load(f)

    cluster_id = config['cluster_id']

    # Terminate the cluster
    terminate_cluster(cluster_id)
    print(f"Terminating the EMR cluster with ID: {cluster_id}")


if __name__ == '__main__':
    main()