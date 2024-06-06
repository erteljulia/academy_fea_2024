with
    total_de_pedidos as (
        select count(*) as total_pedidos_itens
        from {{ ref('fact_pedido_vendas') }}
    )
select total_pedidos_itens
from total_de_pedidos -- corresponde a todo o período analisado, ou seja, 2011 a 2014
where total_pedidos_itens != 121317
