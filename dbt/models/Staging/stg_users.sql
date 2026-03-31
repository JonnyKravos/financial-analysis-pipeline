SELECT
    CAST(ID AS NUMBER) AS id,
    CAST(CURRENT_AGE AS NUMBER) AS current_age,
    CAST(RETIREMENT_AGE AS NUMBER) AS retirement_age,
    TRY_TO_DATE('01/' || BIRTH_MONTH || '/' || BIRTH_YEAR, 'DD/MM/YYYY') AS birth_year_month,
    GENDER AS gender,
    ADDRESS AS address,
    CAST(LATITUDE AS NUMBER) AS latitude,
    CAST(LONGITUDE AS NUMBER) AS longitude,
    CAST(REPLACE(YEARLY_INCOME, '$', '') AS NUMBER) AS yearly_income,
    CAST(REPLACE(TOTAL_DEBT, '$', '') AS NUMBER) AS total_debt,
    CAST(CREDIT_SCORE AS NUMBER) AS credit_score,
    CAST(NUM_CREDIT_CARDS AS NUMBER) AS num_credit_cards,
FROM {{ source('raw', 'USERS')  }}