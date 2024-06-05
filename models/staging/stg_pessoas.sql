with 
    pessoas as (
        select
            businessentityid as pk_entidade_negocios_id
            , concat(firstname, ' ', middlename, ' ', lastname) as nome_completo 
            , persontype as tipo_pessoa
            , cast(modifieddate as timestamp) as data_modificacao
        from {{ source('adventure_works', 'person') }}
    )
select *
from pessoas
