with 
    clientes as (
        select
            customerid as pk_clientes
            , personid as fk_entidade_negocios_id
            , storeid as fk_loja_id
            , territoryid as fk_territorio_id
            , cast(modifieddate as timestamp) as data_modificacao
        from {{ source('adventure_works', 'customer') }}
    )
select *
from clientes
