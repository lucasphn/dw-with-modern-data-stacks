{{ config(
    materialized='table',
    alias='cadastro_cliente'
) }}


with

cadastro_cliente as (

    select

        id,
        nome,
        data_nascimento,
        cpf,
        telefone,
        email,
        rua,
        numero,
        bairro,
        cidade,
        uf,
        created_at as criado_em


    from {{ ref('silver_cadastro_cliente')}}

)

select *

from cadastro_cliente
