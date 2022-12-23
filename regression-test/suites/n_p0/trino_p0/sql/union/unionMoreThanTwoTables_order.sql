SET enable_vectorized_engine=true;
SET enable_nereids_planner=true;
SET enable_fallback_to_original_planner=false;

SELECT count(*)
FROM nation
UNION ALL
SELECT sum(n_nationkey)
FROM nation
GROUP BY n_regionkey
UNION ALL
SELECT n_regionkey
FROM nation
