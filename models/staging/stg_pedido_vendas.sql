with 
    pedido_vendas as (
        select
            salesorderid as pk_pedido_venda_id
            , customerid as fk_cliente
            , territoryid as fk_territorio_id
            , billtoaddressid as fk_endereco_id
            , creditcardid as fk_cartao_credito_id
            , salespersonid as vendedor_id
            , case 
                when onlineorderflag = 1 then 'online'
                when onlineorderflag = 0 then 'vendedor'
            else 'outros'
            end as flag_canal_pedido
            , case
                when status = 1 then 'Em progresso'
                when status = 2 then 'Aprovado'
                when status = 3 then 'Sem estoque'
                when status = 4 then 'Rejeitado'
                when status = 5 then 'Enviado'
                when status = 6 then 'Cancelado'
            else 'sem status'
            end as status_pedido
            , cast(subtotal as numeric(18,2)) as subtotal_pedido
            , cast(taxamt as numeric(18,2)) as taxa
            , cast(freight as numeric(18,2)) as frete
            , cast(totaldue as numeric(18,2)) as total_pedido
            , cast(orderdate as timestamp) as data_pedido
            , cast(duedate as timestamp) as data_pagamento
            , cast(modifieddate as timestamp) as data_modificacao
        from {{ source('adventure_works', 'salesorderheader') }}
    )
select *
from pedido_vendas
