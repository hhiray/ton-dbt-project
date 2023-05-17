{{ config(
    materialized='ephemeral',
    tags='credit_and_cards'
    ) }}

WITH 

dim_date AS (
SELECT 
explode(sequence(to_date('2021-12-05'), date(DATE_ADD(current_Date(), 365)), interval 1 day)) AS days
),

first_transaction AS (
SELECT
customer_id,
MIN(transacted_at) AS first_transaction_at

FROM {{ ref('t_card_transactions_fact')}}
GROUP BY customer_id
),

-- CTE para pegar todos os meses até a data vigente a partir da sua primeira transação
months_of_customers AS (
SELECT DISTINCT
DATE_FORMAT(days, 'yyyy-MM') AS month,
DATE_FORMAT(days, 'yyyy-MM-dd') AS date,
CASE WHEN days = last_day(days) THEN True
WHEN days = to_date(FROM_UTC_TIMESTAMP(NOW(), 'UTC-3')) THEN True
ELSE False
END AS is_last_day,
customer_id AS customer_id

FROM dim_date AS d
LEFT JOIN first_transaction AS ft
  ON DATE_FORMAT(d.days, 'yyyy-MM') BETWEEN DATE_FORMAT(ft.first_transaction_at, 'yyyy-MM') AND DATE_FORMAT(NOW(), 'yyyy-MM')
ORDER BY customer_id, month, date
),

-- CTE para trazer as atividades mês a mês do cliente
months_of_activity AS (
SELECT DISTINCT
DATE_FORMAT(ds.days, 'yyyy-MM-dd') AS date,
customer_id AS customer_id

FROM {{ ref('t_cards_transactions_fact')}} AS t
CROSS JOIN dim_date AS ds
WHERE date(authorization_at) BETWEEN DATE_SUB(ds.days, 30) AND ds.days
ORDER BY customer_id, date
),

base AS (
SELECT
mo.month,
CASE WHEN ft.first_transaction_at IS NOT NULL THEN true ELSE false END AS is_ft_mau_month,
mo.date,
ma.date AS user_activity_date,
mo.is_last_day,
mo.customer_id AS customer_id_base,
ma.customer_id,
CASE WHEN ma.customer_id IS NOT NULL THEN 'Ativo' ELSE NULL END AS activity

FROM months_of_customers AS mo
LEFT JOIN months_of_activity AS ma
  ON ma.date = mo.date AND mo.customer_id = ma.customer_id
LEFT JOIN first_transaction AS ft
  ON DATE_FORMAT(first_transaction_at, 'yyyy-MM') = mo.month AND ft.customer_id = mo.customer_id
WHERE mo.customer_id IS NOT NULL
AND mo.is_last_day = true
ORDER BY customer_id_base, month, date
)

SELECT 
date,
customer_id_base AS customer_id,
CASE WHEN is_ft_mau_month = true AND activity = 'Ativo' THEN 'New Active'
     WHEN LAG(activity) OVER(PARTITION BY customer_id_base ORDER BY date) IS NULL AND activity = 'Ativo' THEN 'Reactivated'
     WHEN activity IS NULL THEN 'Lapsed'
     ELSE 'Active'
     END AS activity_status_ds

FROM base
ORDER BY customer_id_base, date