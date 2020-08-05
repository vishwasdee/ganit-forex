import airflow
from airflow import DAG
from datetime import datetime, timedelta
from airflow.operators.bash_operator import BashOperator
from airflow.operators.python_operator import PythonOperator
from airflow.operators.hive_operator import HiveOperator
from airflow.contrib.operators.spark_submit_operator import SparkSubmitOperator
import requests
import csv
import json


start_date='2018-01-01' #start date of Historical Data needs to be downloaded
end_date='2020-08-06'   #End date of Historical data 
curr_choice=['EUR', 'USD', 'JPY', 'CAD', 'GBP', 'NZD','INR'] #Currencies that are needed out of all

default_dag_arguements = {
    "owner":"Airflow",
    "start_date": datetime(2019, 12, 22),
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "email": "vishwasdee@gmail.com",
    "retries": 1,
    "retry_delay": timedelta(minutes=5)
}

def download_rates():
    download = requests.get(f'https://api.exchangeratesapi.io/history?start_at={start_date}&end_at={end_date}').json()
    with open('/usr/local/airflow/dags/files/rates_raw.json', 'w+') as raw_file:
        json.dump(download, raw_file)
        raw_file.write('\n')

def filter_currency():
    with open('/usr/local/airflow/dags/files/rates_raw.json','rb') as raw_file:
        data=json.load(raw_file)
        result= {k2:{k1:{k:v for k,v in v1.items() if k in curr_choice} for k1,v1 in v2.items()} for k2,v2 in data.items() if k2=="rates"}
        data["rates"]=result["rates"]
        with open('/usr/local/airflow/dags/files/rates_filtered.json', 'w+') as filtered_file:
            json.dump(data, filtered_file)
            filtered_file.write('\n')



with DAG(
    dag_id="ganit_currency_pipeline",
    schedule_interval=None,
    default_args=default_dag_arguements,
    catchup=False) as dag:

#downloading the exchange rates in json format using api.
    download_exchange_rates=PythonOperator(
        task_id="download_exchange_rates",
        python_callable=download_rates

    )
#filtering out the unwanted currencies from the raw file
    filter_exchange_rates=PythonOperator(
        task_id="filter_exchange_rates",
        python_callable=filter_currency
    )

# Saving rates_filtered.json into HDFS to be able to create hive table on it.
    file2hdfs = BashOperator(
        task_id="file2hdfs",
        bash_command="""
            hdfs dfs -mkdir -p /forex && \
            hdfs dfs -put -f /usr/local/airflow/dags/files/rates_filtered.json /forex
            """
    )

#creating hive table with required fields(postgres db is used for hive metastore)
#hive_conn is the connection variable declared in airflow connections list
    create_hive_table = HiveOperator(
        task_id="create_hive_table",
        hive_cli_conn_id="hive_conn",
        hql='/scripts/exchange_rates_ddl.hql',
        dag=dag
    )

#Processing the file for duplicates in spark and inserting into hive table.
    process_exchange_rates= SparkSubmitOperator(
        task_id="process_exchange_rates",
        conn_id="spark_conn",
        application="/usr/local/airflow/dags/scripts/forex_processing.py",
        verbose=False
    )


download_exchange_rates >> filter_exchange_rates >> file2hdfs >> create_hive_table >> process_exchange_rates
