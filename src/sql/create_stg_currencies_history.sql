-- STV2024041049__STAGING.users definition
-- DROP TABLE STV2024041049__STAGING.currencies_history;

CREATE TABLE STV2024041049__STAGING.currencies_history (
    currency_code varchar(200) NOT NULL,
    currency_code_with varchar(200) NOT NULL,
    currency_with_div float(2) NOT NULL,
    date_update date NOT NULL
);

CREATE PROJECTION STV2024041049__STAGING.currencies_history
/*+createtype(P)*/
(
    currency_code,
    currency_code_with,
    currency_with_div,
    date_update
) AS
SELECT
    currency_code,
    currency_code_with,
    currency_with_div,
    date_update
FROM
    STV2024041049__STAGING.currencies_history
ORDER BY
    currency_code SEGMENTED BY hash(currency_code) ALL NODES KSAFE 1;

ALTER TABLE
    STV2024041049__STAGING.currencies_history
ADD
    CONSTRAINT currencies_history_unique UNIQUE (currency_code, currency_code_with, date_update);