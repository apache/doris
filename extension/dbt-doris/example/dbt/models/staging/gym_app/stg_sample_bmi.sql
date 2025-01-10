{{ config(
   post_hook='drop table if exists {{ this.schema }}.{{ this.name }}__dbt_tmp' ,

   properties = {
        "dynamic_partition.time_unit":"DAY",
        "dynamic_partition.end":"8",
        "dynamic_partition.prefix":"p",
        "dynamic_partition.buckets":"4",
        "dynamic_partition.create_history_partition":"true",
        "dynamic_partition.history_partition_num":"20"
   }
)

}}

with

source_height as (

    select * from dbt_testing_example.raw_height

),

source_weight as (

    select * from dbt_testing_example.raw_weight

),

bmi as (

    select
        h.user_id,
        h.height,
        w.weight,
        h.date,
        h.height_unit,
        w.weight_unit,
        {{ calculate_metric_bmi('h.height', 'w.weight', 'h.height_unit', 'w.weight_unit') }} as bmi

    from source_height h
    full outer join source_weight w
    on h.user_id = w.user_id
    where h.date = w.date

)

select * from bmi
