with 
    pessoas as (
        select
            pk_entidade_negocios_id
            , nome_completo 
            , case
                when tipo_pessoa = 'SC' then 'Contato loja'
                when tipo_pessoa = 'IN' then 'Cliente individual varejo'
                when tipo_pessoa = 'SP' then 'Vendedor'
                when tipo_pessoa = 'EM' then 'Funcionário'
                when tipo_pessoa = 'VC' then 'Fornecedor'
                when tipo_pessoa = 'GC' then 'Contato geral'
                else 'outros'
            end as pessoa_segmentacao
        from {{ ref('stg_pessoas') }}
    )
    , clientes as (
        select
            pk_clientes
            , fk_entidade_negocios_id
            , fk_territorio_id
        from {{ ref('stg_clientes') }}
    )
    , joined as (
        select
            {{ dbt_utils.generate_surrogate_key(['pk_entidade_negocios_id', 'pk_clientes']) }} as sk_clientes
            , clientes.pk_clientes
            , clientes.fk_entidade_negocios_id
            , clientes.fk_territorio_id
            , pessoas.nome_completo
            , pessoas.pessoa_segmentacao
        from clientes
        left join pessoas on pessoas.pk_entidade_negocios_id = clientes.fk_entidade_negocios_id
    )
select *
from joined
