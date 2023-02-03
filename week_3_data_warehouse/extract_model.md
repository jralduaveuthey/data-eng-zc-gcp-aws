## Model deployment
[Tutorial](https://cloud.google.com/bigquery-ml/docs/export-model-tutorial)
### Steps
```
# In VS code or terminal with windows	powershell
- \week_3_data_warehouse> gcloud auth login
# create a bucket called taxi_ml_model-375211 in your gcs before the next command
- \week_3_data_warehouse> bq --project_id dtc-de-375211 extract -m nytaxi.tip_model gs://taxi_ml_model-375211/tip_model
- \week_3_data_warehouse> mkdir ./tmp/model
- \week_3_data_warehouse> gsutil cp -r gs://taxi_ml_model-375211/tip_model ./tmp/model
- \week_3_data_warehouse> mkdir -p serving_dir/tip_model/1
- \week_3_data_warehouse> cp -r tmp/model/tip_model/* serving_dir/tip_model/1
- \week_3_data_warehouse> docker pull tensorflow/serving

# NOTE: you have to run the following command in GIT BASH!
- \week_3_data_warehouse> docker run -p 8501:8501 --mount type=bind,source=C:/Users/jraldua-veuthey/Documents/Github_NoPycharm/data-eng-zc-gcp-aws/week_3_data_warehouse/serving_dir/tip_model,target=/models/tip_model -e MODEL_NAME=tip_model -t tensorflow/serving &

# In VS code or terminal with windows	powershell
- \week_3_data_warehouse> docker ps

# NOTE: you have to run the following command in postman desktop
- GET http://localhost:8501/v1/models/tip_model
- POST http://localhost:8501/v1/models/tip_model:predict
# with Body raw and JSON: 
{"instances": [{"passenger_count":1, "trip_distance":12.2, "PULocationID":"193", "DOLocationID":"264", "payment_type":"2","fare_amount":20.4,"tolls_amount":0.0}]}

# You get the same result running this in GIT BASH
- \week_3_data_warehouse> curl -d '{"instances": [{"passenger_count":1, "trip_distance":12.2, "PULocationID":"193", "DOLocationID":"264", "payment_type":"2","fare_amount":20.4,"tolls_amount":0.0}]}' -X POST http://localhost:8501/v1/models/tip_model:predict

```