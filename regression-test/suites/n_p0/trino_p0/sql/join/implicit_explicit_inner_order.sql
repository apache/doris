SET enable_vectorized_engine=true;
SET enable_nereids_planner=true;
SET enable_fallback_to_original_planner=false;

SELECT p_partkey,
       n_name,
       r_name
FROM   nation,
       region
       JOIN part
         ON r_regionkey = p_partkey
WHERE  n_nationkey = r_regionkey

