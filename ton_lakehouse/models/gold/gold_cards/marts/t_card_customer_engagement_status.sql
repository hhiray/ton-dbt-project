{{
    config(
      materialized='table',
      alias='card_customer_engagement_status',
      tags='cards',
      schema='gold_cards'
    )
}}

SELECT 
*

FROM {{ ref( 'stg_card_customer_engagement_status') }}
