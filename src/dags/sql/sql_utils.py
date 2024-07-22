PREPARE_GLOBAL_METRICS = """
    with tmp_by_currency as (
        select
        	distinct
        	t.*, 
        	case
        		when (t.currency_code = '<USD_CURRENCY_CODE>') then 1
        		else ch.currency_with_div
        	end as currency_with_div
        from
        	STV2024041049__STAGING.transactions t
        join STV2024041049__STAGING.currencies_history ch on
        	t.currency_code = ch.currency_code
        	and t.transaction_dt::date = ch.date_update
        where
        	t.account_number_from > 0
        	and status = 'done'
        	and t.transaction_dt::date = '<SELECT_DATE>'
        	and (t.currency_code = '<USD_CURRENCY_CODE>'
        		or currency_code_with = '<USD_CURRENCY_CODE>')
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