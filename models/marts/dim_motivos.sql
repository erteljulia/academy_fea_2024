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
            {{ dbt_utils.generate_surrogate_key(['motivo.pk_motivo', 'motivos_pedidos.fk_pedido_venda_id']) }} as sk_motivo
            , motivos_pedidos.fk_pedido_venda_id
            , array_to_string(array_agg(cast(pk_motivo as string)), ', ') as id_motivos_agregados
            , array_to_string(array_agg(nome_motivo), ', ') as motivos_agregados
            , motivo.tipo_motivo
        from motivo
        left join motivos_pedidos on motivos_pedidos.fk_motivo = motivo.pk_motivo
        group by motivo.pk_motivo, motivos_pedidos.fk_pedido_venda_id, motivo.tipo_motivo
    )
select *
from joined
