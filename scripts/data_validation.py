import boto3
import great_expectations as gx
import os

ge_url = "/mnt/d/Projects/Project-2-End-to-End-Retail-Analytics-Pipeline/great_expectations"

context = gx.data_context.FileDataContext.create(ge_url)

datasource_name = "retailitics_raw_data"
bucket_name = "ete-retailitics-storage-bucket"
boto3_options = {"region_name":"ap-southeast-2"}

datasource = context.add

asset_name = "retail_analytics_asset"
s3_prefix = "ete-retailitics-storage-bucket/data/raw/transactions"
batching_regex = r"transactions_(\d{4}-\d{2}-\d{2})\.parquet"