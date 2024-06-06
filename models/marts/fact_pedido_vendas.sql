with 
    clientes as (
        select
            sk_clientes
            , pk_clientes
    from {{ ref('dim_clientes') }}
    )
    , cartao as (
        select
            sk_cartao
            , pk_cartao_credito_id
    from {{ ref('dim_cartao') }}
    )
    , localizacao as (
        select
            localizacao_sk
            , pk_endereco_id
    from {{ ref('dim_localizacao') }}
    )
    , motivos as (
        select 
            fk_pedido_venda_id
    from {{ ref('dim_motivos') }}
    )
    , produto as (
        select
            produto_sk
            , pk_produto
        from {{ ref('dim_produtos') }}
    )
    , pedido_vendas_itens as (
        select
            sk_pedidos_vendas_itens
            , pk_pedido_venda_id
            , pk_pedido_venda_item
            , fk_produto
            , fk_cliente
            , fk_territorio_id
            , fk_endereco_id
            , fk_cartao_credito_id
            , flag_canal_pedido
            , status_pedido
            , subtotal_pedido
            , total_pedido
            , quantidade_itens
            , preco_unitario
            , desconto_unitario
            , quantidade_itens * preco_unitario as valor_bruto
            , quantidade_itens * (1 - desconto_unitario) * preco_unitario as valor_liquido
            , data_pedido
            , data_pagamento
            , data_modificacao
        from {{ ref('int_pedidos_vendas_itens') }}
    )
    , joined as (
        select
            clientes.sk_clientes
            , cartao.sk_cartao
            , localizacao.localizacao_sk
            , produto.produto_sk
            , pedido_vendas_itens.pk_pedido_venda_id
            , pedido_vendas_itens.pk_pedido_venda_item
            , pedido_vendas_itens.fk_produto
            , pedido_vendas_itens.fk_cliente
            , pedido_vendas_itens.fk_endereco_id
            , pedido_vendas_itens.fk_cartao_credito_id
            , pedido_vendas_itens.flag_canal_pedido
            , pedido_vendas_itens.status_pedido
            , cast (
                 pedido_vendas_itens.subtotal_pedido / count(pedido_vendas_itens.pk_pedido_venda_id) over (partition by pedido_vendas_itens.pk_pedido_venda_id)
                 as numeric(18,2)
            ) as subtotal_rateado
            , cast(
                pedido_vendas_itens.total_pedido / count(pedido_vendas_itens.pk_pedido_venda_id) over (partition by pedido_vendas_itens.pk_pedido_venda_id)
                as numeric(18,2)
            ) as total_pedido_rateado
            , pedido_vendas_itens.quantidade_itens
            , pedido_vendas_itens.preco_unitario
            , pedido_vendas_itens.desconto_unitario
            , pedido_vendas_itens.valor_bruto
            , pedido_vendas_itens.valor_liquido
            , pedido_vendas_itens.data_pedido
            , pedido_vendas_itens.data_pagamento
            , pedido_vendas_itens.data_modificacao
        from pedido_vendas_itens
        left join produto on pedido_vendas_itens.fk_produto = produto.pk_produto
        left join clientes on pedido_vendas_itens.fk_cliente = clientes.pk_clientes
        left join cartao on pedido_vendas_itens.fk_cartao_credito_id = cartao.pk_cartao_credito_id
        left join localizacao on pedido_vendas_itens.fk_endereco_id = localizacao.pk_endereco_id
        left join motivos on pedido_vendas_itens.pk_pedido_venda_id = motivos.fk_pedido_venda_id
    )
    , with_sk as (
        select 
            {{ dbt_utils.generate_surrogate_key(['pk_pedido_venda_id', 'pk_pedido_venda_item']) }} as sk_fato_pedidos
            , *
        from joined
    )

select *
from with_sk
