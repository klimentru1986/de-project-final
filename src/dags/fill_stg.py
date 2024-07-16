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

    @task(task_id="update_currencies_history_stg")
    def update_currencies_history_stg():
        prepare_sql = """
            DROP TABLE IF EXISTS STV2024041049__STAGING.currencies_history_tmp;
            CREATE TABLE STV2024041049__STAGING.currencies_history_tmp LIKE STV2024041049__STAGING.currencies_history INCLUDING PROJECTIONS;
        """

        copy_sql = """    
            COPY STV2024041049__STAGING.currencies_history (currency_code,currency_code_with,date_update,currency_with_div)
            FROM LOCAL '/data/currencies_history.csv'
            DELIMITER ','
            SKIP 1
            REJECTED DATA AS TABLE STV2024041049__STAGING.currencies_history_rej; 
        """

        merge_sql = """
            MERGE INTO STV2024041049__STAGING.currencies_history tgt
            USING STV2024041049__STAGING.currencies_history_tmp src 
            ON (tgt.currency_code = src.currency_code 
                AND tgt.currency_code_with = src.currency_code_with 
                AND tgt.date_update=src.date_update)
            WHEN NOT MATCHED THEN INSERT VALUES (src.currency_code, src.currency_code_with, src.currency_with_div, src.date_update); 
        """

        try_execute(prepare_sql)
        try_execute(copy_sql)
        try_execute(merge_sql)

    currencies_history = fetch_currencies_history()
    transactions_batch = fetch_transactions_batch()
    currencies_history_stg = update_currencies_history_stg()

    (start >> currencies_history >> currencies_history_stg >> end)
    (start >> transactions_batch >> end)


fill_stg = fill_stg_dag()
