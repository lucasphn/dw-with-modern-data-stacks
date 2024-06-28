{{ config(
    materialized='table',
    alias='cadastro_medicos'
) }}


with

cadastro_medico as (

    select

        id,
        nome,
        area_atuacao as area_de_atuacao,
        created_at as criado_em


    from {{ ref('silver_cadastro_medico')}}

)

select *

from cadastro_medico
