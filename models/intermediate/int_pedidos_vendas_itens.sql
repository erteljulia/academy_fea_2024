with
    pedido_vendas as (
        select *
        from {{ ref('stg_pedido_vendas') }}
    )
    , pedido_vendas_itens as (
        select *
        from {{ ref('stg_pedido_vendas_itens') }}
    )
    , joined as (
        select
            {{ dbt_utils.generate_surrogate_key(['pedido_vendas.pk_pedido_venda_id', 'pedido_vendas_itens.fk_produto']) }} as sk_pedidos_vendas_itens
            , pedido_vendas.pk_pedido_venda_id
            , pedido_vendas_itens.pk_pedido_venda_item
            , pedido_vendas_itens.fk_produto
            , pedido_vendas.fk_cliente
            , pedido_vendas.fk_territorio_id
            , pedido_vendas.fk_endereco_id
            , pedido_vendas.fk_cartao_credito_id
            , pedido_vendas.vendedor_id
            , pedido_vendas.flag_canal_pedido
            , pedido_vendas.status_pedido
            , cast(
                pedido_vendas.total_pedido / count(pk_pedido_venda_id) over (partition by pk_pedido_venda_id)
                as numeric(18,2)
            ) as total_pedido_rateado
            , pedido_vendas_itens.quantidade_itens
            , pedido_vendas_itens.preco_unitario
            , pedido_vendas_itens.desconto_unitario
            , pedido_vendas.data_pedido
            , pedido_vendas.data_pagamento
            , pedido_vendas.data_modificacao
        from pedido_vendas
        left join pedido_vendas_itens on pedido_vendas.pk_pedido_venda_id = pedido_vendas_itens.fk_pedido_venda_id
    )
select *
from joined
