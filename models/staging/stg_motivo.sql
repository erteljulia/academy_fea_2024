with 
    motivo as (
        select
            salesreasonid as pk_motivo
            , name as nome_motivo
            , reasontype as tipo_motivo
            , cast(modifieddate as timestamp) as data_modificacao
        from {{ source('adventure_works', 'salesreason') }}
    )
select *
from motivo 
