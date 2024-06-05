with 
    pedido_vendas_itens as (
        select
            salesorderid as fk_pedido_venda_id
            , salesorderdetailid as pk_pedido_venda_item
            , productid as fk_produto
            , orderqty as quantidade_itens
            , cast(unitprice as numeric (18,2)) as preco_unitario
            , cast(unitpricediscount as numeric (18,2)) as desconto_unitario
            , cast(modifieddate as timestamp) as data_modificacao
        from {{ source('adventure_works', 'salesorderdetail') }}
    )
select *
from pedido_vendas_itens
