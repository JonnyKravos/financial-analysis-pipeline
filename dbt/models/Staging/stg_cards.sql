SELECT
    CAST(ID AS NUMBER) AS id,
    CAST(CLIENT_ID AS NUMBER) AS client_id,
    CARD_BRAND AS card_brand,
    CARD_TYPE AS card_type,
    CARD_NUMBER AS card_number,
    TRY_TO_DATE('01/' || EXPIRES, 'DD/MM/YYYY') AS expires,
    CVV AS cvv,
    CAST(HAS_CHIP AS BOOLEAN) as has_chip,
    CAST(NUM_CARDS_ISSUED AS NUMBER) AS num_cards_issued,
    CAST(REPLACE(CREDIT_LIMIT, '$', '') AS NUMBER) AS credit_limit,
    TRY_TO_DATE('01/' || ACCT_OPEN_DATE, 'DD/MM/YYYY') AS acct_open_date,
    CAST(YEAR_PIN_LAST_CHANGED AS NUMBER) AS year_pin_last_changed,
    CAST(CARD_ON_DARK_WEB AS BOOLEAN) AS card_on_dark_web
FROM {{ source('raw', 'CARDS')  }}