with 
    endereco as (
        select
            addressid as pk_endereco_id
            , stateprovinceid as fk_estado_id
            , city as cidade
            , addressline1 as endereco
            , cast(modifieddate as timestamp) as data_modificacao
        from {{ source('adventure_works', 'address') }}
    )
select *
from endereco
