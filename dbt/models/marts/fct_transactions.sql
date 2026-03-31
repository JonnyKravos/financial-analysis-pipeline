SELECT
    id AS transaction_id,
    client_id AS customer_id,
    amount AS amount_dollars,
    card_id,
    date AS transaction_date
FROM {{ ref('stg_transactions') }}