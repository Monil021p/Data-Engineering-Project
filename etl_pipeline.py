from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import requests
import json

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2024, 4, 1),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'api_to_postgres_pipeline',
    default_args=default_args,
    description='ETL pipeline to extract from API, transform and load into PostgreSQL',
    schedule_interval='@daily',
    catchup=False
)

def extract_data():
    url = "https://api.coingecko.com/api/v3/simple/price?ids=bitcoin&vs_currencies=usd"
    response = requests.get(url)

    if response.status_code == 200:
        data = response.json()
        price = data["bitcoin"]["usd"]

        result = {
            "currency": "bitcoin",
            "usd_price": price,
            "timestamp": datetime.utcnow().isoformat()
        }

        with open('/tmp/raw_data.json', 'w') as f:
            json.dump(result, f)
        print("✅ Data extracted and saved to /tmp/raw_data.json")
    else:
        raise Exception(f"❌ Failed to fetch data: {response.status_code}")

def transform_data():
    with open('/tmp/raw_data.json', 'r') as f:
        data = json.load(f)

    # Example transformation: convert USD price to integer cents
    transformed = {
        "currency": data["currency"],
        "usd_price_cents": int(data["usd_price"] * 100),
        "timestamp": data["timestamp"]
    }

    with open('/tmp/transformed_data.json', 'w') as f:
        json.dump(transformed, f)

    print("🔁 Data transformed and saved to /tmp/transformed_data.json")

def load_data():
    pass  # soon

extract_task = PythonOperator(task_id='extract_data', python_callable=extract_data, dag=dag)
transform_task = PythonOperator(task_id='transform_data', python_callable=transform_data, dag=dag)
load_task = PythonOperator(task_id='load_data', python_callable=load_data, dag=dag)

extract_task >> transform_task >> load_task

