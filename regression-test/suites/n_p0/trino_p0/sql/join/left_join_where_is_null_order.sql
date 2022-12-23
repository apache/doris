SET enable_vectorized_engine=true;
SET enable_nereids_planner=true;
SET enable_fallback_to_original_planner=false;

SELECT n_name
FROM   nation
       LEFT JOIN region
              ON n_nationkey = r_regionkey
WHERE  r_name is null

