import logging
from datetime import datetime

import pandas as pd
from airflow.decorators import dag, task
from airflow.operators.empty import EmptyOperator
from utils.read_s3 import read_s3
from utils.vertica_utils import try_execute

logger = logging.getLogger(__name__)


@dag(start_date=datetime(2024, 4, 1), schedule_interval="@daily", catchup=False)
def fill_stg_dag():
    start = EmptyOperator(task_id="start")
    end = EmptyOperator(task_id="end")

    @task(task_id="currencies_history")
    def fetch_currencies_history():
        logger.info(read_s3("currencies_history.csv").head())

    @task(task_id="transactions_batch")
    def fetch_transactions_batch():
        i = 1
        while True:
            try:
                logger.info(read_s3(f"transactions_batch_{i}.csv").head())
                i += 1
            except Exception as e:
                logger.info(e)
                logger.info(f"transactions_batch index: {i}")
                break

    currencies_history = fetch_currencies_history()
    transactions_batch = fetch_transactions_batch()

    (start >> [currencies_history, transactions_batch] >> end)


fill_stg = fill_stg_dag()
