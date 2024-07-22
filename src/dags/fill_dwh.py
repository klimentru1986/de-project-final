import logging
from datetime import datetime, timedelta

import pandas as pd
from airflow.decorators import dag, task
from airflow.operators.empty import EmptyOperator
from sql.sql_utils import PREPARE_GLOBAL_METRICS
from utils.vertica_utils import try_execute

logger = logging.getLogger(__name__)


def get_dt(ctx):
    return ctx["ti"].xcom_pull(key="return_value", task_ids="get_dt")


@dag(
    start_date=datetime(2022, 10, 1),
    end_date=datetime(2022, 11, 1),
    schedule_interval="@daily",
    max_active_runs=1,
    catchup=True,
)
def fill_dwh_dag():
    start = EmptyOperator(task_id="start")
    end = EmptyOperator(task_id="end")

    @task(task_id="get_dt")
    def get_dt_task(**context):
        DT_FORMAT = "%Y-%m-%d"
        logger.info(context["ds"])
        current_date = datetime.strptime(context["ds"], DT_FORMAT)
        prev_day = current_date - timedelta(days=1)

        return prev_day.strftime(DT_FORMAT)

    @task(task_id="prepare_data")
    def prepare_data(**context):
        select_date = get_dt(context)
        USD_CURRENCY_CODE = "430"

        sql = PREPARE_GLOBAL_METRICS.replace(
            "<USD_CURRENCY_CODE>", USD_CURRENCY_CODE
        ).replace("<SELECT_DATE>", select_date)

        logger.info(sql)

        res = try_execute(sql)
        df = pd.DataFrame(
            res,
            columns=[
                "date_update",
                "currency_from",
                "amount_total",
                "cnt_transactions",
                "avg_transactions_per_account",
                "cnt_accounts_make_transactions",
            ],
        )

        df.to_csv(f"/data/global_metrics_{select_date}.csv", index=False)
        logger.info(df)

    @task(task_id="update_dwh")
    def update_dwh(**context):
        select_date = get_dt(context)

        prepare_sql = """
            DROP TABLE IF EXISTS STV2024041049__DWH.global_metrics_tmp;
            CREATE TABLE STV2024041049__DWH.global_metrics_tmp LIKE STV2024041049__DWH.global_metrics INCLUDING PROJECTIONS;
        """

        copy_sql = f"""    
            COPY STV2024041049__DWH.global_metrics_tmp (date_update, currency_from, amount_total, cnt_transactions, avg_transactions_per_account, cnt_accounts_make_transactions)
            FROM LOCAL '/data/global_metrics_{select_date}.csv'
            DELIMITER ','
            SKIP 1
            REJECTED DATA AS TABLE STV2024041049__DWH.global_metrics_rej; 
        """

        merge_sql = """
            MERGE INTO STV2024041049__DWH.global_metrics tgt
            USING STV2024041049__DWH.global_metrics_tmp src 
            ON (tgt.currency_from = src.currency_from AND tgt.date_update=src.date_update)
            WHEN NOT MATCHED THEN INSERT VALUES (
                src.date_update, 
                src.currency_from, 
                src.amount_total, 
                src.cnt_transactions, 
                src.avg_transactions_per_account, 
                src.cnt_accounts_make_transactions
            ); 
        """

        try_execute(prepare_sql)
        try_execute(copy_sql)
        try_execute(merge_sql)

    dt = get_dt_task()
    data = prepare_data()
    dwh = update_dwh()

    start >> dt >> data >> dwh >> end


dwh_dag = fill_dwh_dag()
