{{ config(
    materialized='table',
    alias='agenda_medica'
) }}


with

agenda_medica as (

    select

        id,
        price,
        description,
        nome_medico,
        data_agendada,
        created_at

    from {{ ref('silver_agenda_medica')}}

),

agenda_medica_renamed as (

    select

        id,
        price as preco,
        description as descricao ,
        nome_medico as nome_do_medico,
        data_agendada as data_do_agendamento,
        created_at criado_em

    from agenda_medica

)

select *

from agenda_medica_renamed
