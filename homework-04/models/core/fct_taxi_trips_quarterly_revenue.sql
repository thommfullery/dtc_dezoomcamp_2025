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
        min(pickup_year) pickup_year,
        min(pickup_quarter) pickup_quarter,
        pickup_year_quarter,
        sum(total_amount) revenue
      from
        trips_data
     group by
        service_type,
        pickup_year_quarter
)
    select
        qrc.service_type,
        qrc.pickup_year,
        qrc.pickup_quarter,
        qrc.pickup_year_quarter,
        qrc.revenue,
        case
            when qrp.revenue is null then 0.0
            else qrp.revenue
        end prior_revenue,
        case
            when qrp.revenue is null then 0.0
            else (qrc.revenue - qrp.revenue) / qrp.revenue
        end revenue_growth,
        case
            when qrp.revenue is null then 'N/A'
            else CONCAT(CAST(ROUND(100.0 * (qrc.revenue - qrp.revenue) / qrp.revenue, 2) AS STRING), '%')
        end revenue_growth_percent
      from
        qr qrc
 left join
        qr qrp
        on
        qrp.service_type = qrc.service_type
       and
        qrp.pickup_year = qrc.pickup_year - 1
       and
        qrp.pickup_quarter = qrc.pickup_quarter

-- dbt build --vars '{'is_test_run': 'false'}'