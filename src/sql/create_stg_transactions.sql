CREATE TABLE STV2024041049__STAGING.transactions (
    operation_id varchar(60) NULL,
    account_number_from int NULL,
    account_number_to int NULL,
    currency_code varchar(200) NULL,
    country varchar(30) NULL,
    status varchar(30) NULL,
    transaction_type varchar(30) NULL,
    amount int NULL,
    transaction_dt timestamp NULL
) PARTITION BY ((transaction_dt) :: date);

CREATE PROJECTION STV2024041049__STAGING.transactions
/*+createtype(P)*/
(
    operation_id,
    account_number_from,
    account_number_to,
    currency_code,
    country,
    status,
    transaction_type,
    amount,
    transaction_dt
) AS
SELECT
    transactions.operation_id,
    transactions.account_number_from,
    transactions.account_number_to,
    transactions.currency_code,
    transactions.country,
    transactions.status,
    transactions.transaction_type,
    transactions.amount,
    transactions.transaction_dt
FROM
    STV2024041049__STAGING.transactions
ORDER BY
    account_number_from SEGMENTED BY hash(account_number_from) ALL NODES KSAFE 1;