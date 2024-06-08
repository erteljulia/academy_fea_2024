salesorderwith 
    motivo_pedidos as (
        select
            salesorderid as fk_pedido_venda_id
            , salesreasonid as fk_motivo
            , cast(modifieddate as timestamp) as data_modificacao
        from {{ source('adventure_works', 'salesorderheadersalesreason') }}
    )
select *
from motivo_pedidos
