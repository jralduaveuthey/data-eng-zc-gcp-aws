# DE Zoomcamp 5.6.3 - Setting up a Spark Cluster in AWS EMR 
## With Python
Run the following in this directory:
- create an emr_cluster.json from the emr_config_TEMPLATE.json with your configuration
- ``python .\create_cluster.py``
- ``python .\run_pyspark_in_cluster.py``
- ``python .\terminate_cluster.py``

## With Cloudformation
Run the following in this directory:
- create an emr_cluster.json from the emr_config_TEMPLATE.json with your configuration
- ``python .\create_cluster.py``
- ``python .\run_pyspark_redshift_in_cluster.py`` #TODO: here it is not working and returns with status FAILED. See Events in Cluster. 
- ``python .\terminate_cluster.py``

# DE Zoomcamp 5.6.4 - Connecting Spark to RedShift
#TODO: test 