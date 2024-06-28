{{ config(
    materialized='view',
    alias='silver_empregos_jaragua_do_sul'
) }}


with

source as (

    select

        id,
        nome_da_vaga,
        nome_da_empresa,
        data_publicada,
        numero_de_vagas,
        quantidade_vagas,
        senioridade,
        formato_disponibilidade,
        area_de_atuacao,
        cidade,
        regiao,
        logo_da_empresa,
        saber_mais,
        created_at

    from {{ source('dw-with-mordern-data-stacks', 'bronze_scraping_empregos_jgua')}}
)


select * from source
