SET enable_vectorized_engine=true;
SET enable_nereids_planner=true;
SET enable_fallback_to_original_planner=false;

SELECT p_partkey,
       n_name,
       r_name
FROM   part
       INNER JOIN nation
               ON n_regionkey = p_partkey
       RIGHT JOIN region
               ON n_nationkey = r_regionkey

