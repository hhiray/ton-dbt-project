{{ config(
    materialized='ephemeral',
    tags='cards'
    ) }}


SELECT
-- The following CONCAT is to create an unique PK to the transactions
CONCAT(TO_UNIX_TIMESTAMP(transacted_at), customer_id, transaction_id) AS transaction_unique_id,
transaction_id,
customer_id

FROM {{ source('ton_bronze', 'card_transactions') }}