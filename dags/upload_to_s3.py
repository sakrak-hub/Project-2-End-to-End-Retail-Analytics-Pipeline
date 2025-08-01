import airflow
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
import boto3
from datetime import datetime, timedelta
import os
import subprocess

today = datetime.now()
S3_BUCKET_NAME = 'ete-retailitics-storage-bucket'
S3_RAW_KEY = 'data/raw'
LOCAL_DATA_PATH = './retail_data_v2'
INSTANCE_ID = 'i-009a5f98335c002f0'

default_args = {
    'owner': 'data_engineer',
    'depends_on_past': True,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

def generate_retail_data(execution_date: str, **_):
    """Call the generator with Y M D taken from the DAG run date."""
    dt = datetime.fromisoformat(execution_date)
    y, m, d = dt.strftime("%Y"), dt.strftime("%m"), dt.strftime("%d")
    subprocess.run(["uv", "run", "scripts/data_generator_2.py", y, m, d], check=True)

def upload_to_s3(local_dir: str, bucket_name: str, execution_date: str, **_):
    """Upload all files produced for this run to a partitioned S3 folder."""
    dt_str = datetime.fromisoformat(execution_date).strftime("%Y-%m-%d")
    s3 = boto3.client("s3")

    static_list = ["customers.parquet", "products.parquet", "stores.parquet"]

    for fname in os.listdir(local_dir):
        fpath = os.path.join(local_dir, fname)
        if not os.path.isfile(fpath):
            continue

        if fname in static_list:
            key = f"{S3_RAW_KEY}/static/{fname}"
            s3.upload_file(fpath, bucket_name, key)
            continue

        if fname.startswith(f"transactions_{dt_str}"):
            key = f"{S3_RAW_KEY}/transactions/{dt_str}/{fname}"
            s3.upload_file(fpath, bucket_name, key)
            continue
        elif fname.startswith(f"daily_summary_{dt_str}"):
            key = f"{S3_RAW_KEY}/transactions/{dt_str}/{fname}"
            s3.upload_file(fpath, bucket_name, key)
            continue
        else:
            continue

        # if fname.startswith(f"transactions_{dt_str}"):
        #     key = f"{S3_RAW_KEY}/transactions/{dt_str}/{fname}"
        # elif fname.startswith("daily_summary"):
        #     key = f"{S3_RAW_KEY}/daily_summary/{dt_str}/{fname}"
        # else:
        #     key = f"{S3_RAW_KEY}/static/{fname}"

            s3.upload_file(fpath, bucket_name, key)
    
    

def start_instance():
    ec2 = boto3.client("ec2", region_name="ap-southeast-2")
    ec2.start_instances(InstanceIds=[INSTANCE_ID])
    waiter = ec2.get_waiter("instance_running")
    waiter.wait(InstanceIds=[INSTANCE_ID])  
    return "instance running"

def stop_instance():
    ec2 = boto3.client("ec2", region_name="ap-southeast-2")
    ec2.stop_instances(InstanceIds=[INSTANCE_ID])
    waiter = ec2.get_waiter("instance_stopped")
    waiter.wait(InstanceIds=[INSTANCE_ID])  
    return "instance_stopped"

with DAG(
    dag_id="upload_to_s3",
    start_date=datetime(2025, 1, 1),
    schedule="@daily",
    catchup=True,
    max_active_runs=1,
    default_args=default_args,
) as dag:

    start = PythonOperator(
        task_id = "start_ec2", 
        python_callable = start_instance
        )

    generate_data = PythonOperator(
        task_id="generate_retail_data",
        # bash_command=(
        #     "uv run scripts/data_generator.py "
        #     "{{ execution_date.strftime('%Y') }} "
        #     "{{ execution_date.strftime('%m') }} "
        #     "{{ execution_date.strftime('%d') }}"
        #     ),
        python_callable = generate_retail_data,
        op_kwargs={
            "execution_date" : "{{ ds }}"
        }
    )

    upload = PythonOperator(
        task_id = "upload_to_bucket", 
        python_callable = upload_to_s3, 
        op_kwargs={
            "local_dir": LOCAL_DATA_PATH, 
            "bucket_name": S3_BUCKET_NAME,
            "execution_date": "{{ ds }}"
            }
        )

    stop = PythonOperator(
        task_id = "stop_ec2", 
        python_callable = stop_instance,
        trigger_rule = 'all_done'
        )

    start >> generate_data >> upload >> stop