{{
    config(
        materialized='table'
    )
}}

with trips_data as (
    select * from {{ ref('fact_trips') }}
),
qr as (
    select
        service_type,
        pickup_year_quarter,
        sum(total_amount) revenue
      from
        trips_data
     group by
        service_type,
        pickup_year_quarter
)
select
    *
  from
    qr