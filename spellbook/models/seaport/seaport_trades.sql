{{ config(
        alias='trades',
        materialized ='incremental')
}}

SELECT blockchain, 
    'seaport' as project, 
    'v1' as version, 
    tx_hash, 
    block_time, 
    amount_usd, 
    amount, 
    token_symbol, 
    token_address, 
    unique_trade_id 
FROM 
(SELECT 'ethereum' as blockchain, 
    tx_hash, 
    block_time, 
    usd_amount as amount_usd, 
    original_amount as amount, 
    original_currency as token_symbol, 
    original_currency_contract as token_address, 
    tx_hash || '-' || evt_index::string as unique_trade_id
    FROM {{ ref('seaport_ethereum_view_transactions') }} ) 
{% if is_incremental() %}
-- this filter will only be applied on an incremental run
WHERE block_time > now() - interval 2 days
{% endif %} 