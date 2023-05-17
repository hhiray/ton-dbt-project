{{
    config(
      materialized='table',
      alias='transactions_fact',
      tags='cards',
      schema='silver_cards'
    )
}}

SELECT 
*

FROM {{ ref( 'stg_cards_transactions_fact') }}
