import logging
from datetime import datetime, timedelta

import pandas as pd
from airflow.decorators import dag, task
from airflow.operators.empty import EmptyOperator
from utils.read_s3 import read_s3
from utils.vertica_utils import try_execute

logger = logging.getLogger(__name__)


@dag(
    start_date=datetime(2022, 10, 1),
    end_date=datetime(2022, 10, 3),
    schedule_interval="@daily",
    catchup=True,
)
def fill_dwh_dag2():
    start = EmptyOperator(task_id="start")
    end = EmptyOperator(task_id="end")

    @task(task_id="get_dt")
    def get_dt(**context):
        DT_FORMAT = "%Y-%m-%d"
        logger.info(context["ds"])
        current_date = datetime.strptime(context["ds"], DT_FORMAT)
        prev_day = current_date - timedelta(days=1)

        return prev_day.strftime(DT_FORMAT)

    @task(task_id="prepare_data")
    def prepare_data(**context):
        select_date = context["ti"].xcom_pull(key="return_value", task_ids="get_dt")

        USD_CURRENCY_CODE = "430"

        sql = f"""
        with tmp_by_currency as (
            select
            	distinct
            	t.*, 
            	case
            		when (t.currency_code = '{USD_CURRENCY_CODE}') then 1
            		else ch.currency_with_div
            	end as currency_with_div
            from
            	STV2024041049__STAGING.transactions t
            join STV2024041049__DWH.global_metrics ch on
            	t.currency_code = ch.currency_code
            	and t.transaction_dt::date = ch.date_update
            where
            	t.account_number_from > 0
            	and status = 'done'
                and transaction_dt::date = '{select_date}'
            	and (t.currency_code = '{USD_CURRENCY_CODE}'
            		or currency_code_with = '{USD_CURRENCY_CODE}')
        ),
        tmp_by_acc as (
            select
            	currency_code, 
            	transaction_dt::date, 
            	account_number_from,
            	sum(amount * currency_with_div) as amount_total,
            	count(operation_id) as cnt_transactions_per_acc
            from
            	tmp_by_currency
            WHERE
            	account_number_from > 0
            	and status = 'done'
            GROUP BY
            	currency_code,
            	transaction_dt::date,
            	account_number_from
        ), 
        result as (
            select
            		transaction_dt as date_update,
            		currency_code as currency_from,
            		sum(amount_total) as amount_total,
            		sum(cnt_transactions_per_acc) as cnt_transactions,
            		avg(cnt_transactions_per_acc) as avg_transactions_per_account,
            		count(distinct account_number_from) as cnt_accounts_make_transactions
            from
            	tmp_by_acc
            group by
            	currency_code,
            	transaction_dt
        )
        select
        	*
        from
        	result;     
        """
        
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
        select_date = context["ti"].xcom_pull(key="return_value", task_ids="get_dt")

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

    dt = get_dt()
    data = prepare_data()
    dwh = update_dwh()

    start >> dt >> data >> dwh >> end


dwh_dag2 = fill_dwh_dag2()
