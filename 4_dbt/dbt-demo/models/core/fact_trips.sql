{{
    config(
        materialized='table'
    )
}}

WITH yellow_tripdata AS (
    select *, 
        'Yellow' as service_type
    from {{ ref('stg_yellow_tripdata') }}
), 
dim_zones as (
    select * from {{ ref('dim_zones') }}
    where borough != 'Unknown'
)
select yellow_tripdata.tripid, 
    yellow_tripdata.vendorid, 
    yellow_tripdata.service_type,
    yellow_tripdata.ratecodeid, 
    yellow_tripdata.pickup_locationid, 
    pickup_zone.borough as pickup_borough, 
    pickup_zone.zone as pickup_zone, 
    yellow_tripdata.dropoff_locationid,
    dropoff_zone.borough as dropoff_borough, 
    dropoff_zone.zone as dropoff_zone,  
    yellow_tripdata.pickup_datetime, 
    yellow_tripdata.dropoff_datetime, 
    yellow_tripdata.store_and_fwd_flag, 
    yellow_tripdata.passenger_count, 
    yellow_tripdata.trip_distance, 
    yellow_tripdata.trip_type, 
    yellow_tripdata.fare_amount, 
    yellow_tripdata.extra, 
    yellow_tripdata.mta_tax, 
    yellow_tripdata.tip_amount, 
    yellow_tripdata.tolls_amount, 
    yellow_tripdata.ehail_fee, 
    yellow_tripdata.improvement_surcharge, 
    yellow_tripdata.total_amount, 
    yellow_tripdata.payment_type, 
    yellow_tripdata.payment_type_description
from yellow_tripdata
inner join dim_zones as pickup_zone
on yellow_tripdata.pickup_locationid = pickup_zone.locationid
inner join dim_zones as dropoff_zone
on yellow_tripdata.dropoff_locationid = dropoff_zone.locationid