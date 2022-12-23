SET enable_vectorized_engine=true;
SET enable_nereids_planner=true;
SET enable_fallback_to_original_planner=false;

SELECT p_partkey,
       n_name
FROM   nation
       LEFT JOIN part
              ON n_nationkey = p_partkey
WHERE  n_name < p_name 

