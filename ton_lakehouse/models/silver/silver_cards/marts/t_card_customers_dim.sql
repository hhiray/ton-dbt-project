{{
    config(
      materialized='table',
      alias='customers_dim',
      tags='cards',
      schema='silver_cards'
    )
}}

SELECT 
*

FROM {{ ref( 'stg_card_transactions_dim') }}
