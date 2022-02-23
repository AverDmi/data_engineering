{{ config(materialized='table') }}

select * from {{ source('staging', 'external_fhv_data') }}