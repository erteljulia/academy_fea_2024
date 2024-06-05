with 
    cartao as (
        select
            pk_cartao_credito_id
            , tipo_cartao
        from {{ ref('stg_cartao') }}
    )
    , with_sk as (
        select 
            {{ dbt_utils.generate_surrogate_key(['pk_cartao_credito_id', 'tipo_cartao']) }} as sk_cartao
            , pk_cartao_credito_id
            , tipo_cartao
        from cartao
    )
select *
from with_sk
