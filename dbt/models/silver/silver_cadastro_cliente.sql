{{ config(
    materialized='view',
    alias='silver_cadastro_cliente'
) }}


with

source as (

    select

        id,
        uf,
        cpf,
        rua,
        nome,
        email,
        bairro,
        cidade,
        numero,
        telefone,
        created_at,
        data_nascimento


    from {{ source('dw-with-mordern-data-stacks', 'bronze_cadastro_cliente')}}
)

select * from source
