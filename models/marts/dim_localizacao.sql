with 
    endereco as (
        select
            pk_endereco_id
            , fk_estado_id
            , cidade
            , endereco
            , data_modificacao
        from {{ ref('stg_endereco') }}
    )
    , estado as (
        select
            pk_estado_id
            , fk_codigo_pais
            , fk_territorio_id
            , nome_estado
            , data_modificacao
        from {{ ref('stg_estado') }}
    )
    , pais as (
        select
            pk_codigo_pais
            , nome_pais
            , data_modificacao
        from {{ ref('stg_pais') }}
    )
    , transformed as (
        select
            {{ dbt_utils.generate_surrogate_key(['endereco.pk_endereco_id' , 'estado.pk_estado_id']) }} as localizacao_sk
            , endereco.pk_endereco_id
            , estado.fk_codigo_pais
            , estado.fk_territorio_id
            , endereco.fk_estado_id
            , endereco.cidade
            , endereco.endereco
            , estado.nome_estado
            , pais.nome_pais
        from estado
        left join pais on pais.pk_codigo_pais = estado.fk_codigo_pais
        left join endereco on endereco.fk_estado_id = estado.pk_estado_id
    )
select *
from transformed
