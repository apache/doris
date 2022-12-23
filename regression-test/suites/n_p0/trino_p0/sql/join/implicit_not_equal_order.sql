SET enable_vectorized_engine=true;
SET enable_nereids_planner=true;
SET enable_fallback_to_original_planner=false;

SELECT n_name,
       r_name
FROM   nation,
       region
WHERE  r_regionkey != n_nationkey 

