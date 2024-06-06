with 
    motivo as (
        select 
            pk_motivo
            , nome_motivo
            , tipo_motivo
        from {{ ref('stg_motivo') }}
    )
    , motivos_pedidos as (
        select
            fk_pedido_venda_id
            , fk_motivo
        from {{ ref('stg_motivos_pedidos') }}
    )
    , joined as (
        select 
            motivos_pedidos.fk_pedido_venda_id
            , listagg(motivo.nome_motivo, ', ') within group (order by motivo.nome_motivo) as motivos_agregados
            , listagg(to_varchar(motivo.pk_motivo), ', ') within group (order by motivo.pk_motivo) as id_motivos_agregados
            , listagg(motivo.tipo_motivo, ', ') within group (order by motivo.tipo_motivo) as tipo_motivo
        from motivo
        left join motivos_pedidos on motivos_pedidos.fk_motivo = motivo.pk_motivo
        group by motivos_pedidos.fk_pedido_venda_id
    )
select *
from joined
