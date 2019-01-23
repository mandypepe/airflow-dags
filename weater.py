# Borre todos los datos existentes en la carpeta / weather_csv / en HDFS.
# Copie los archivos CSV de la carpeta ~ / data a la carpeta / weather_csv / en HDFS.
# Convierte los datos CSV en HDFS en formato ORC usando Hive.
# Exporte los datos en formato ORC utilizando Presto al formato Microsoft Excel 2013.
from datetime import timedelta

import airflow
from airflow.hooks.presto_hook import PrestoHook
from airflow.operators.bash_operator import BashOperator
from airflow.operators.python_operator import PythonOperator
import numpy  as np
import pandas as pd

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': airflow.utils.dates.days_ago(0),
    'email': ['airflow@example.com'],
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 3,
    'retry_delay': timedelta(minutes=15),
}

dag = airflow.DAG('weather',
                  default_args=default_args,
                  description='Copy weather data to HDFS & dump to Excel',
                  schedule_interval=timedelta(minutes=30))

cmdr = "hdfs dfs -rm /weather_csv/*.csv || true"
remove_csvs_task = BashOperator(task_id='remove_csvs',
                                bash_command=cmdr,
                                dag=dag)

cmdc = """hdfs dfs -copyFromLocal  /home/mark/data/us-weather-history/*.csv  /weather_csv/"""
csv_to_hdfs_task = BashOperator(task_id='csv_to_hdfs',
                                bash_command=cmdc,
                                dag=dag)

cmdi = """echo "INSERT INTO weather_orc SELECT * FROM weather_csv;" | hive"""
csv_to_orc_task = BashOperator(task_id='csv_to_orc',
                               bash_command=cmdi,
                               dag=dag)


def presto_to_excel(**context):
    column_names = [
        "date",
        "actual_mean_temp",
        "actual_min_temp",
        "actual_max_temp",
        "average_min_temp",
        "average_max_temp",
        "record_min_temp",
        "record_max_temp",
        "record_min_temp_year",
        "record_max_temp_year",
        "actual_precipitation",
        "average_precipitation",
        "record_precipitation"
    ]

    sql = """SELECT *
             FROM weather_orc
             LIMIT 20"""

    ph = PrestoHook(catalog='hive',
                    schema='default',
                    port=8080)
    data = ph.get_records(sql)

    df = pd.DataFrame(np.array(data).reshape(20, 13),
                      columns=column_names)

    writer = pd.ExcelWriter('weather.xlsx',
                            engine='xlsxwriter')
    df.to_excel(writer, sheet_name='Sheet1')
    writer.save()

    return True


presto_to_excel_task = PythonOperator(task_id='presto_to_excel',
                                      provide_context=True,
                                      python_callable=presto_to_excel,
                                      dag=dag)

remove_csvs_task >> csv_to_hdfs_task >> csv_to_orc_task >> presto_to_excel_task

if __name__ == "__main__":
    dag.cli()
