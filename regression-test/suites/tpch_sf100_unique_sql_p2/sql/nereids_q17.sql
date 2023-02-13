SET enable_nereids_planner=TRUE;
SET enable_fallback_to_original_planner=FALSE;
-- tables: lineitem,part
SELECT sum(l_extendedprice) / 7.0 AS avg_yearly
FROM
  lineitem,
  part
WHERE
  p_partkey = l_partkey
  AND p_brand = 'Brand#23'
  AND p_container = 'MED BOX'
  AND l_quantity < (
    SELECT 0.2 * avg(l_quantity)
    FROM
      lineitem
    WHERE
      l_partkey = p_partkey
  )
