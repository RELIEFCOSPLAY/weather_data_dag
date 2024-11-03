# %%
from airflow import DAG
from airflow.providers.http.operators.http import SimpleHttpOperator
from airflow.operators.python import PythonOperator
from airflow.providers.http.sensors.http import HttpSensor
from airflow.utils.dates import days_ago
from airflow.providers.postgres.operators.postgres import PostgresOperator

import json

# %%
# Define default arguments for the DAG
default_args = {
    "owner": "airflow",
    "start_date": days_ago(1),
    "retries": 1,
}
# %%
# Initialize the DAG
with DAG(
    "weather_data_dag",
    default_args=default_args,
    schedule_interval="@daily",  # This will run the DAG daily
    catchup=False,
) as dag:

    # 1. Check if the API is available
    check_api = HttpSensor(
        task_id="check_weather_api",
        http_conn_id="weather_api",
        endpoint="current.json?key=a7b78630443c40f8b1b135649240311&q=Bangkok",  # Modify to the actual API endpoint
        poke_interval=5,
        timeout=20,
    )
    # %%
    # 2. Extract weather data
    extract_weather_data = SimpleHttpOperator(
        task_id="extract_weather_data",
        http_conn_id="weather_api",
        endpoint="current.json?key=a7b78630443c40f8b1b135649240311&q=Bangkok",
        method="GET",
        response_filter=lambda response: response.json(),
        log_response=True,
    )

    # %%
    # 3. Transform the data
    def transform_weather_data(ti):
        raw_data = ti.xcom_pull(task_ids="extract_weather_data")

        # Extract only specific fields from the response
        transformed_data = {
            "temperature": raw_data["current"]["temp_c"],
            "humidity": raw_data["current"]["humidity"],
            "weather": raw_data["current"]["condition"]["text"],
            "timestamp": raw_data["current"]["last_updated_epoch"],
        }

        # Push the transformed data to XCom
        ti.xcom_push(key="transformed_data", value=transformed_data)

    transform_data = PythonOperator(
        task_id="transform_data",
        python_callable=transform_weather_data,
    )
    # %%
    # 4. Load data into MySQL
    load_data = PostgresOperator(
        task_id="load_data_to_postgresql",
        postgres_conn_id="postgres_default",
        sql="""
        CREATE TABLE IF NOT EXISTS weather_data (
            id SERIAL PRIMARY KEY,
            temperature FLOAT NOT NULL,
            humidity INT NOT NULL,
            weather VARCHAR(255) NOT NULL,
            timestamp TIMESTAMP NOT NULL
        );

        INSERT INTO weather_data (temperature, humidity, weather, timestamp)
        VALUES ({{ ti.xcom_pull(task_ids='transform_data', key='transformed_data')['temperature'] }},
                {{ ti.xcom_pull(task_ids='transform_data', key='transformed_data')['humidity'] }},
                '{{ ti.xcom_pull(task_ids='transform_data', key='transformed_data')['weather'] }}',
                TO_TIMESTAMP({{ ti.xcom_pull(task_ids='transform_data', key='transformed_data')['timestamp'] }}));
    """,
    )

    # Define task dependencies
    extract_weather_data >> transform_data >> load_data

# %%
