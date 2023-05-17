{{ config(
    materialized='ephemeral',
    tags='cards'
    ) }}


SELECT
-- The following CONCAT is to create an unique PK to the transactions
CONCAT(TO_UNIX_TIMESTAMP(transacted_at), customer_id, transaction_id) AS transaction_unique_id,
capture_type,
card_banner,
payment_type,
CASE WHEN transaction_state IN ('REFUNDED', 'Rrefunded') THEN 'REFUNDED'
     WHEN transaction_state IN ('CHARGEDBACK', 'chargedback') THEN 'CHARGEDBACK'
     ELSE transaction_state
END AS transaction_state,
transaction_value,
transacted_at

FROM {{ source('ton_bronze', 'card_transactions') }}