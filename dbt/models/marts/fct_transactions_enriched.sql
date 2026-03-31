SELECT
    t.transaction_id,
    t.transaction_date,
    t.amount_dollars,
    t.customer_id,
    t.card_id,

    u.birth_year_month,

    c.card_brand,
    c.card_type,
    c.credit_limit

FROM {{ ref('fct_transactions') }} t

LEFT JOIN {{ ref('dim_users') }} u
    ON t.customer_id = u.user_id

LEFT JOIN {{ ref('dim_cards') }} c
    ON t.card_id = c.card_id