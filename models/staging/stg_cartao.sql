with 
    cartao_credito as (
        select
            creditcardid as pk_cartao_credito_id
            , cardtype as tipo_cartao
            , cast(modifieddate as timestamp) as data_modificacao
        from {{ source('adventure_works', 'creditcard') }}
    )
select *
from cartao_credito
