# Info
Here information about how to reproduce week 4 with Redshift and AWS.  


# Steps
1. Create a new Github repo cause dbt cloud needs to work in the root directory of the repo (and this repo "data-eng-zc-gcp-aws" was already used in dbt cloud following week 4 in BQ) => https://github.com/jralduaveuthey/dtc-de-zc-week4-redshift  
2. Create a new dbt account (=> "dtc-de-zc-week4-redshift") and a new project (=> "Analytics") in dbt cloud (https://cloud.getdbt.com/)
3. Set up the project as explained in week_4_analytics_engineering\dbt_cloud_setup.md

# Inside dbt cloud
1. `dbt init`
2. Replace name in dbt_project.yml with "taxi_rides_ny" and delete `example:   materialized: view`  
3. Run the scriptweek_4_analytics_engineering\aws\prefect_flows\web_to_s3_to_redshift.py to create the tables in Redshift. You need to create the tables first in Redshift but for that there is also code in the script (commented inside the function write_redshift) 
4. If you want to follow the same steps as in the zoomcamp you can follow 4.3.1 and 4.3.2, or you can copy from week_4_analytics_engineering\taxi_rides_ny the content from the folders macros, models and data. And also copy the content of the files dbt_project.yml and packages.yml. Additionally you need to add taxi_zone_lookup.csv to the seeds folder. Then run `dbt deps` and `dbt build` in dbt cloud.
5. Create a "production" schema in your database "nytaxi" in the Redshift cluster. Follow steps video 4.4.1 to create Production environment in dbt cloud.
<td> <img src="imgs\Capture5.PNG" style="width: 600px;"/> </td>