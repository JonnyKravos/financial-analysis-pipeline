SELECT
    CAST(ID AS NUMBER) AS id,
    CAST(CLIENT_ID AS NUMBER) AS client_id,
    CAST(REPLACE(AMOUNT, '$', '') AS NUMBER) AS amount,
    CAST(CARD_ID AS NUMBER) AS card_id,
    CAST("DATE" as DATE) AS date
FROM {{ source('raw', 'TRANSACTIONS')   }}