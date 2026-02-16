with source as (
  select *
  from {{ source('raw', 'fhv_tripdata_2019_ext') }}
),

staged as (
  select
    -- keep only valid records
    dispatching_base_num,

    -- rename common fields (adjust names if your raw columns differ)
    pickup_datetime as pickup_datetime,
    dropoff_datetime as dropoff_datetime,

    -- location fields (rename to match conventions)
    PUlocationID as pickup_location_id,
    DOlocationID as dropoff_location_id

  from source
  where dispatching_base_num is not null
)

select * from staged
