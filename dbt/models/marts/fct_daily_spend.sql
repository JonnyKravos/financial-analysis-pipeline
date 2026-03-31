SELECT
    transaction_date,
    SUM(amount_dollars) AS total_spend,
    COUNT(*) AS transaction_count,
    AVG(amount_dollars) AS avg_transaction_amount
FROM {{ ref('fct_transactions') }}
GROUP BY transaction_date