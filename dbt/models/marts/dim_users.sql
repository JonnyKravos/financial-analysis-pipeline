SELECT
    id AS user_id,
    current_age AS age,
    birth_year_month,
    gender,
    address,
    yearly_income,
    total_debt,
    credit_score,
    num_credit_cards,
FROM {{ ref('stg_users') }}