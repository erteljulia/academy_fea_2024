with
    vendas_brutas_2011 as (
        select sum(valor_bruto) as total_bruto
        from {{ ref('fact_pedido_vendas') }}
        where extract(year from data_pedido) = 2011 
    )
select total_bruto
from vendas_brutas_2011 -- $12.646.112,16 esse é o valor repassado pelo CEO
where total_bruto != 12646112.1607
