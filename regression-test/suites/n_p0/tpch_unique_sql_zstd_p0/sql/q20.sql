-- tables: supplier,nation,partsupp,lineitem,part
SET enable_vectorized_engine=true;
SET enable_nereids_planner=true;
SET enable_fallback_to_original_planner=false;

SELECT
  s_name,
  s_address
FROM
  supplier, nation
WHERE
  s_suppkey IN (
    SELECT ps_suppkey
    FROM
      partsupp
    WHERE
      ps_partkey IN (
        SELECT p_partkey
        FROM
          part
        WHERE
          p_name LIKE 'forest%'
      )
      AND ps_availqty > (
        SELECT 0.5 * sum(l_quantity)
        FROM
          lineitem
        WHERE
          l_partkey = ps_partkey
          AND l_suppkey = ps_suppkey
          AND l_shipdate >= date('1994-01-01')
          AND l_shipdate < date('1994-01-01') + interval '1' YEAR
)
)
AND s_nationkey = n_nationkey
AND n_name = 'CANADA'
ORDER BY s_name
