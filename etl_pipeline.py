from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import requests
import json
import psycopg2

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2024, 4, 1),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'multi_crypto_currency_etl',
    default_args=default_args,
    description='ETL pipeline for multiple cryptocurrencies and multi-currency prices',
    schedule='@hourly',
    catchup=False
)

def extract_data():
    ids = "bitcoin,ethereum,tether"
    url = f"https://api.coingecko.com/api/v3/simple/price?ids={ids}&vs_currencies=usd,inr,eur"
    response = requests.get(url)

    if response.status_code == 200:
        data = response.json()
        timestamp = datetime.utcnow().isoformat()

        records = []
        for currency, prices in data.items():
            records.append({
                "currency": currency,
                "usd_price": prices.get("usd", 0),
                "inr_price": prices.get("inr", 0),
                "eur_price": prices.get("eur", 0),
                "timestamp": timestamp
            })

        with open('/tmp/raw_data.json', 'w') as f:
            json.dump(records, f)

        print("Data for multiple cryptocurrencies extracted successfully.")
    else:
        raise Exception(f"Failed to fetch data: {response.status_code}")

def transform_data():
    with open('/tmp/raw_data.json', 'r') as f:
        raw_data = json.load(f)

    transformed_data = []
    for record in raw_data:
        transformed_data.append({
            "currency": record["currency"],
            "usd_price_cents": int(record["usd_price"] * 100),
            "inr_price_paise": int(record["inr_price"] * 100),
            "eur_price_cents": int(record["eur_price"] * 100),
            "timestamp": record["timestamp"]
        })

    with open('/tmp/transformed_data.json', 'w') as f:
        json.dump(transformed_data, f)

    print("Transformed data saved.")

def load_data():
    with open('/tmp/transformed_data.json', 'r') as f:
        data = json.load(f)

    conn = psycopg2.connect(
        dbname="airflow_data",
        user="postgres",
        password="7867",
        host="localhost",
        port="5432"
    )
    cursor = conn.cursor()

    insert_query = """
    INSERT INTO crypto_prices2 (
        currency, usd_price_cents, inr_price_paise, eur_price_cents, timestamp
    ) VALUES (%s, %s, %s, %s, %s)
    """

    for row in data:
        cursor.execute(insert_query, (
            row['currency'],
            row['usd_price_cents'],
            row['inr_price_paise'],
            row['eur_price_cents'],
            row['timestamp']
        ))

    conn.commit()
    cursor.close()
    conn.close()
    print("All records inserted into PostgreSQL.")

extract_task = PythonOperator(task_id='extract_data', python_callable=extract_data, dag=dag)
transform_task = PythonOperator(task_id='transform_data', python_callable=transform_data, dag=dag)
load_task = PythonOperator(task_id='load_data', python_callable=load_data, dag=dag)

extract_task >> transform_task >> load_task

