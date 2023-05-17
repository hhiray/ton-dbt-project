{{ config(
    materialized='ephemeral',
    tags='cards'
    ) }}


SELECT
customer_id,
customer_state,
customer_city

FROM {{ source('ton_bronze', 'card_transactions') }}