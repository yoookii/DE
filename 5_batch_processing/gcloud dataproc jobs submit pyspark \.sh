gcloud dataproc jobs submit pyspark \
  --project=inlaid-reactor-427616-m8 \
  --cluster=de-cluster \
  --region=us-central1 \
  --jars=gs://spark-lib/bigquery/spark-3.5-bigquery-0.39.1.jar \
  gs://mage-de-demo/code/06_spark_bigquery.py \
  -- \
    --input_green="gs://mage-de-demo/pq/green/2020/*/" \
    --input_yellow="gs://mage-de-demo/pq/yellow/2020/*/" \
    --output="inlaid-reactor-427616-m8.trips_data_all.report-2020"