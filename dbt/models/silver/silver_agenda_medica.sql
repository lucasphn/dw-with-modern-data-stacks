{{ config(
    materialized='view',
    alias='silver_agenda_medica'
) }}


with

source as (

    select

        id,
        price,
        description,
        nome_medico,
        data_agendada,
        created_at

    from {{ source('dw-with-mordern-data-stacks', 'bronze_agenda_medica')}}
)

select * from source
