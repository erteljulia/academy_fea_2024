with 
    produto as (
        select
            distinct pk_produto
            , fk_subcategoria
            , nome_produto
            , custo
            , preco_venda
            , data_modificacao
        from {{ ref('stg_produtos') }}
    )
    , with_sk as (
            select 
            {{ dbt_utils.generate_surrogate_key(['pk_produto']) }} as produto_sk
            , pk_produto
            , nome_produto
            , custo
            , preco_venda
            , data_modificacao
        from produto
    )
select *
from with_sk
