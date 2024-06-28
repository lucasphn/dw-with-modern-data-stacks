{{ config(
    materialized='view',
    alias='silver_cadastro_medico'
) }}


with

source as (

    select

        id,
        nome,
        area_atuacao,
        created_at

    from {{ source('dw-with-mordern-data-stacks', 'bronze_cadastro_profissional')}}
)

select * from source
