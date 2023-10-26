#Kelompok 1:
#1. Khairunisa
#3. M. Arif Rahman
#4. Noer Amalia Puspita
#5. Nurhayati
#6. Rahman Zuhri Maulana


from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import Variable

import requests
import pandas as pd
from datetime import datetime
import sqlalchemy 

# 1
# Ingest data from the OpenAQ API (https://docs.openaq.org/docs) to retrieve data on the current air quality around the world.
def fetch_data_from_api(**context):
    url = Variable.get("endpoint_latest_data_id") # get value from the Airflow Variable
    headers = {"accept": "application/json", 
               "content-type": "application/json"
    }
    response = requests.get(url, headers=headers)
    return context['ti'].xcom_push(key='api_result', value=response.json())

# 2
# Transform the data to filter only data from Jakarta,Indonesia                               
def filter_data_only_jaksel(**context):
    print(context['ti'].xcom_pull(key='api_result'))
    response_json = context['ti'].xcom_pull(key='api_result')

    for i in range(len(response_json['results'])):
        location = response_json['results'][i]['location']
        if 'Jakarta-Selatan-CasaGrande' in location:
            data_jaksel_casegrande = response_json['results'][i]

    return context['ti'].xcom_push(key='data_jaksel_api', value=data_jaksel_casegrande)

# 3
# Transform it to a format that can be stored in a database table.
def transform_data_api(**context):
    data_jaksel = context['ti'].xcom_pull(key='data_jaksel_api')

    # Extract location, city, and country
    location = data_jaksel['location']
    city = data_jaksel['city']
    country = data_jaksel['country']

    # Create a DataFrame from measurements
    df = pd.DataFrame(data_jaksel['measurements'])

    # Add location, city, and country columns to the DataFrame
    df['location'] = location
    df['city'] = city
    df['country'] = country

    # Convert 'lastUpdated' column to datetime object
    df['lastUpdated'] = pd.to_datetime(df['lastUpdated'])

    # Format datetime as string with timezone offset suitable for PostgreSQL
    df['lastUpdated'] = df['lastUpdated'].dt.strftime('%Y-%m-%d %H:%M:%S')

    # Reorder columns
    new_column_order = ['location', 'country', 'city', 'parameter', 'value', 'unit', 'lastUpdated']
    latest_df = df[new_column_order]

    return context['ti'].xcom_push(key='latest_df', value=latest_df)

# 4
# Create a PostgreSQL connection that can be used to ingest data into a database
# Define a task to fetch data from the PostgreSQL database
def fetch_data_from_psql(**context):
    # Establish a connection using the Airflow connection ID
    hook = PostgresHook(postgres_conn_id="postgresql_db") 
    
    # Define your SQL query
    sql_query = 'SELECT * FROM public.latest'
    
    # Fetch data from the database
    exist_data = hook.get_pandas_df(sql_query)

    return context['ti'].xcom_push(key='exist_data_psql', value=exist_data)

def transform_data_psql(**context):
    exist_data = context['ti'].xcom_pull(key='exist_data_psql')

    # change datatype lastUpdated to datetime
    exist_data['lastUpdated'] = pd.to_datetime(exist_data['lastUpdated'])
    exist_data['lastUpdated'] = exist_data['lastUpdated'].dt.strftime('%Y-%m-%d %H:%M:%S')

    return context['ti'].xcom_push(key='data_psql_new', value=exist_data)

# Define a task to merge DataFrames
def merge_data(**context):
    # Assuming you have already fetched and processed the DataFrames in earlier tasks
    exist_data_psql = context['ti'].xcom_pull(key='data_psql_new')
    latest_df = context['ti'].xcom_pull(key='latest_df')

    # Merge the DataFrames
    merged_df = pd.concat([latest_df, exist_data_psql])

    return context['ti'].xcom_push(key='merged_df', value=merged_df)

# 5
# Store the transformed data into a new database table.
# Define a task to load the DataFrame into PostgreSQL using PostgresHook
def load_data(**context):    
    df = context['ti'].xcom_pull(key='merged_df')

    # Create a PostgresHook with the desired PostgreSQL connection
    hook = PostgresHook(postgres_conn_id="day18_kel11")

    return df.to_sql('dwh_kel11', 
              hook.get_sqlalchemy_engine(), 
              if_exists='replace', 
              index=False,
              dtype={'lastUpdated': sqlalchemy.types.TIMESTAMP()}
    )

default_args = {
    'owner': 'kel11',
    'start_date': datetime(2023, 10, 21),
}

dag = DAG(
    'etl_day18',
    default_args=default_args,
    description='A simple DAG to solve assignment day 18 Dibimbing',
    catchup=False,
    schedule_interval='0 4 * * *',
)

fetch_data_from_api = PythonOperator(
    task_id='fetch_data_from_api',
    python_callable=fetch_data_from_api,
    dag=dag,
)

filter_data_only_jaksel = PythonOperator(
    task_id='filter_data_only_jaksel',
    python_callable=filter_data_only_jaksel,
    provide_context=True,
    dag=dag,
)

transform_data_api = PythonOperator(
    task_id='transform_data_api',
    python_callable=transform_data_api,
    provide_context=True,
    dag=dag,
)

transform_data_psql = PythonOperator(
    task_id='transform_data_psql',
    python_callable=transform_data_psql,
    provide_context=True,
    dag=dag,
)

fetch_data_from_psql = PythonOperator(
    task_id='fetch_data_from_psql',
    python_callable=fetch_data_from_psql,
    provide_context=True,
    dag=dag,
)

merge_data = PythonOperator(
    task_id='merge_data',
    python_callable=merge_data,
    provide_context=True,
    dag=dag,
)

load_data = PythonOperator(
    task_id='load_data',
    python_callable=load_data,
    provide_context=True,
    dag=dag,
)

fetch_data_from_api >> filter_data_only_jaksel >> transform_data_api >> merge_data
fetch_data_from_psql >> transform_data_psql >> merge_data
merge_data >> load_data