-- tables: partsupp,supplier,nation
SET enable_vectorized_engine=true;
SET enable_nereids_planner=true;
SET enable_fallback_to_original_planner=false;

SELECT
  ps_partkey,
  sum(ps_supplycost * ps_availqty) AS value
FROM
  partsupp,
  supplier,
  nation
WHERE
  ps_suppkey = s_suppkey
  AND s_nationkey = n_nationkey
  AND n_name = 'GERMANY'
GROUP BY
  ps_partkey
HAVING
  sum(ps_supplycost * ps_availqty) > (
    SELECT sum(ps_supplycost * ps_availqty) * 0.0001
    FROM
      partsupp,
      supplier,
      nation
    WHERE
      ps_suppkey = s_suppkey
      AND s_nationkey = n_nationkey
      AND n_name = 'GERMANY'
  )
ORDER BY
  value DESC
