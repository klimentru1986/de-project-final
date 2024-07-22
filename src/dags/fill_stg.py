import logging
from datetime import datetime

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
        for i in range(1, 1000):
            try:
                logger.info(read_s3(f"transactions_batch_{i}.csv").head())
            except Exception as e:
                logger.info(e)
                logger.info(f"transactions_batch index: {i - 1}")
                return i - 1

    @task(task_id="update_currencies_history_stg")
    def update_currencies_history_stg():
        prepare_sql = """
            DROP TABLE IF EXISTS STV2024041049__STAGING.currencies_history_tmp;
            CREATE TABLE STV2024041049__STAGING.currencies_history_tmp LIKE STV2024041049__STAGING.currencies_history INCLUDING PROJECTIONS;
        """

        copy_sql = """    
            COPY STV2024041049__STAGING.currencies_history_tmp (currency_code,currency_code_with,date_update,currency_with_div)
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

    @task(task_id="update_transactions_stg")
    def update_transactions_stg(**context):
        batch_count = context["ti"].xcom_pull(
            key="return_value", task_ids="transactions_batch"
        )

        for i in range(1, batch_count):
            prepare_sql = """
                DROP TABLE IF EXISTS STV2024041049__STAGING.transactions_tmp;
                CREATE TABLE STV2024041049__STAGING.transactions_tmp LIKE STV2024041049__STAGING.transactions INCLUDING PROJECTIONS;
            """

            copy_sql = f"""    
                COPY STV2024041049__STAGING.transactions_tmp (operation_id,account_number_from,account_number_to,currency_code,country,status,transaction_type,amount,transaction_dt)
                FROM LOCAL '/data/transactions_batch_{i}.csv'
                DELIMITER ','
                SKIP 1
                REJECTED DATA AS TABLE STV2024041049__STAGING.transactions_rej; 
            """

            merge_sql = """
                MERGE INTO STV2024041049__STAGING.transactions tgt
                USING STV2024041049__STAGING.transactions_tmp src 
                ON (tgt.operation_id = src.operation_id 
                    AND tgt.account_number_from = src.account_number_from
                    AND tgt.account_number_to = src.account_number_to
                    AND tgt.currency_code = src.currency_code
                    AND tgt.transaction_type = src.transaction_type 
                    AND tgt.country = src.country
                    AND tgt.status = src.status
                    AND tgt.transaction_dt=src.transaction_dt)
                WHEN NOT MATCHED THEN INSERT VALUES (
                    src.operation_id,
                    src.account_number_from,
                    src.account_number_to,
                    src.currency_code,
                    src.country,
                    src.status,
                    src.transaction_type,
                    src.amount,
                    src.transaction_dt); 
            """

            try_execute(prepare_sql)
            try_execute(copy_sql)
            try_execute(merge_sql)

    currencies_history = fetch_currencies_history()
    transactions_batch = fetch_transactions_batch()
    currencies_history_stg = update_currencies_history_stg()
    transactions_stg = update_transactions_stg()

    (start >> currencies_history >> currencies_history_stg >> end)
    (start >> transactions_batch >> transactions_stg >> end)


fill_stg = fill_stg_dag()
