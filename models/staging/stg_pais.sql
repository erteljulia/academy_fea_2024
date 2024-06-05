with 
    pais as (
        select
            countryregioncode as pk_codigo_pais
            , name as nome_pais
            , cast(modifieddate as timestamp) as data_modificacao
        from {{ source('adventure_works', 'countryregion') }}
    )
select *
from pais
