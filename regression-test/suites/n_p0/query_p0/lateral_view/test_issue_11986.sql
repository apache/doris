SET enable_vectorized_engine=true;
SET enable_nereids_planner=true;
SET enable_fallback_to_original_planner=false;

select * from (
    select 1 hour,'a' pid_code ,'u1' uid, 10 money
    union all
    select 3 hourr,'a' pid_code ,'u1' uid, 10 money
) example1 lateral view explode_bitmap(bitmap_from_string("1,2,3,4")) tmp1 as e1 where hour=e1;