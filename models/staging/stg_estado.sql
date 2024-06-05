with 
    estado as (
        select
            stateprovinceid as pk_estado_id
            , countryregioncode as fk_codigo_pais
            , name as nome_estado
            , territoryid as fk_territorio_id
            , cast(modifieddate as timestamp) as data_modificacao
        from {{ source('adventure_works', 'stateprovince') }}
    )
select *
from estado
