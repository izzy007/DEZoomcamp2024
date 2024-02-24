{{
    config(
        materialized='table'
    )
}}

with green_tripdata as (
    select *, 
        'Green' as service_type
    from {{ ref('stg_green_tripdata') }}
), 
yellow_tripdata as (
    select *, 
        'Yellow' as service_type
    from {{ ref('stg_yellow_tripdata') }}
), 

unioned_trips as (
    select * from green_tripdata
    union all
    select * from yellow_tripdata

),
dim_zones as (
    select * from {{ ref('dim_zones') }}
    where borough != 'Unknown'
)

select unioned_trips.tripid, 
    unioned_trips.vendorid, 
    unioned_trips.service_type,
    unioned_trips.ratecodeid, 
    unioned_trips.pickup_locationid, 
    pickup_zone.borough as pickup_borough, 
    pickup_zone.zone as pickup_zone, 
    unioned_trips.dropoff_locationid,
    dropoff_zone.borough as dropoff_borough, 
    dropoff_zone.zone as dropoff_zone,  
    unioned_trips.pickup_datetime, 
    unioned_trips.dropoff_datetime, 
    unioned_trips.store_and_fwd_flag, 
    unioned_trips.passenger_count, 
    unioned_trips.trip_distance, 
    unioned_trips.trip_type, 
    unioned_trips.fare_amount, 
    unioned_trips.extra, 
    unioned_trips.mta_tax, 
    unioned_trips.tip_amount, 
    unioned_trips.tolls_amount, 
    unioned_trips.ehail_fee, 
    unioned_trips.improvement_surcharge, 
    unioned_trips.total_amount, 
    unioned_trips.payment_type, 
    unioned_trips.payment_type_description
from unioned_trips
inner join dim_zones as pickup_zone
on unioned_trips.pickup_locationid = pickup_zone.locationid
inner join dim_zones as dropoff_zone
on unioned_trips.dropoff_locationid = dropoff_zone.locationid