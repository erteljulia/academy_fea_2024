with 
    produto as (
        select
            productid as pk_produto
            , name as nome_produto
            , cast(standardcost as numeric(18,2)) as custo
            , cast(listprice as numeric(18,2)) as preco_venda
            , productsubcategoryid as fk_subcategoria
            , cast(modifieddate as timestamp) as data_modificacao
        from {{ source('adventure_works', 'product') }}
    )
select *
from produto
