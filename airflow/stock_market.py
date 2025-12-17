from airflow.decorators import dag, task
from airflow.sensors.base import PokeReturnValue
from airflow.providers.amazon.aws.operators.glue import GlueJobOperator
from airflow.hooks.base import BaseHook
from datetime import datetime
import requests
import pandas as pd
import awswrangler as wr
import boto3

BUCKET = "stockmarket.pipeline"
TICKER = "MFC"


def _get_stock_prices():

     
    api = BaseHook.get_connection("stock_api")
    host = api.host.rstrip("/")
    headers = api.extra_dejson.get("headers", {"User-Agent": "Mozilla/5.0"})

    url = f"{host}/v8/finance/chart/{TICKER}?range=1y&interval=1d"

    print(f"Fetching from: {url}")

    response = requests.get(url, headers=headers, timeout=30)
    response.raise_for_status()

    result = response.json()["chart"]["result"][0]
    df = pd.DataFrame({"full_record": [result]})

    now = datetime.utcnow().strftime("%Y-%m-%d_%H-%M-%S")
    s3_path = f"s3://{BUCKET}/parquet/full_json/stock={TICKER}/{now}.parquet"

     
    aws_conn = BaseHook.get_connection("aws_connection")

    session = boto3.Session(
        aws_access_key_id=aws_conn.login,
        aws_secret_access_key=aws_conn.password,
        region_name=aws_conn.extra_dejson.get("region", "us-east-2")
    )

     
    wr.s3.to_parquet(
        df=df,
        path=s3_path,
        index=False,
        boto3_session=session
    )

    return s3_path


@dag(
    start_date=datetime(2024, 1, 1),
    schedule="@daily",
    catchup=False,
    tags=["stock_market"],
)
def stock_market_pipeline():

    
    @task.sensor(poke_interval=30, timeout=300)
    def is_api_available() -> PokeReturnValue:

        api = BaseHook.get_connection("stock_api")
        host = api.host.rstrip("/")
        headers = api.extra_dejson.get("headers", {"User-Agent": "Mozilla/5.0"})

        url = f"{host}/v8/finance/chart/{TICKER}"
        print(f"Checking API: {url}")

        try:
            response = requests.get(url, headers=headers, timeout=15)
            return PokeReturnValue(is_done=response.status_code == 200)
        except:
            return PokeReturnValue(is_done=False)

     
    @task
    def fetch_stock_data():
        return _get_stock_prices()

     
    glue_job = GlueJobOperator(
        task_id="run_glue_etl",
        job_name="yahoo_stock_etl",
        aws_conn_id="aws_connection",
        region_name="us-east-2",
        wait_for_completion=True
    )

 
    is_api_available() >> fetch_stock_data() >> glue_job


stock_market_pipeline()
