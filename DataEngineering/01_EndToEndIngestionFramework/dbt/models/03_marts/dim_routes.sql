{{ config(materialized='table', schema='gold') }}

select * from {{ ref('int_routes_cleansed') }}