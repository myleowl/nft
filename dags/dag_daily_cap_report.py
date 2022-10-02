from airflow import DAG
from airflow.operators.python import PythonOperator

import datetime
from cap_report import functions

DAG_ID = 'daily_capitalization_reports'


def daily_cap_reports(owner: str,
                      start_date: str,
                      schedule_interval: str) -> DAG:
    args = {'owner': owner,
            'start_date': datetime.datetime.strptime(start_date, '%d.%m.%y'),
            'retries': 0
            }
    with DAG(dag_id=DAG_ID,
             catchup=False,
             default_args=args,
             schedule_interval=schedule_interval) as dag:
        download_csv_task = PythonOperator(
            task_id='download_csv_to_db',
            python_callable=functions.download_csv_to_db_task
        )
        calculate_and_save_report_task = PythonOperator(
            task_id='calculate_and_save_report',
            python_callable=functions.calculate_and_save_report_task
        )

        download_csv_task >> calculate_and_save_report_task

    return dag


dag_dict = {
    "owner": 'alena',
    "start_date": "02.10.22",
    "schedule_interval": "0 7 * * *"
}
globals()['daily_capitalization_reports'] = daily_cap_reports(**dag_dict)
