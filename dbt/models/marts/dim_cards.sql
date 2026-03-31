SELECT
    id AS card_id,
    client_id AS user_id,
    card_brand,
    card_type,
    card_number,
    expires AS expiration_date,
    cvv,
    num_cards_issued,
    credit_limit,
    acct_open_date,
    year_pin_last_changed AS pin_last_changed,
    card_on_dark_web
FROM {{ ref('stg_cards')  }}