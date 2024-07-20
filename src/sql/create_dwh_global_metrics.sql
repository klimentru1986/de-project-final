-- global_metrics
-- date_update — дата расчёта,
-- currency_from — код валюты транзакции;
-- amount_total — общая сумма транзакций по валюте в долларах;
-- cnt_transactions — общий объём транзакций по валюте;
-- avg_transactions_per_account — средний объём транзакций с аккаунта;
-- cnt_accounts_make_transactions — количество уникальных
CREATE TABLE STV2024041049__DWH.global_metrics (
    date_update date NOT NULL,
    currency_from varchar(200) NOT NULL,
    amount_total float(2),
    cnt_transactions int,
    avg_transactions_per_account float(2),
    cnt_accounts_make_transactions int
);

CREATE PROJECTION STV2024041049__DWH.global_metrics
/*+createtype(P)*/
(
    date_update,
    currency_from,
    amount_total,
    cnt_transactions,
    avg_transactions_per_account,
    cnt_accounts_make_transactions
) AS
SELECT
    global_metrics.date_update,
    global_metrics.currency_from,
    global_metrics.amount_total,
    global_metrics.cnt_transactions,
    global_metrics.avg_transactions_per_account,
    global_metrics.cnt_accounts_make_transactions
FROM
    STV2024041049__DWH.global_metrics
ORDER BY
    date_update SEGMENTED BY hash(currency_from) ALL NODES KSAFE 1;