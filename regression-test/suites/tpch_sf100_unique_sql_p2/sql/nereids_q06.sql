SET enable_nereids_planner=TRUE;
SET enable_fallback_to_original_planner=FALSE;
-- tables: lineitem

SELECT sum(l_extendedprice * l_discount) AS revenue
FROM
  lineitem
WHERE
  l_shipdate >= DATE '1994-01-01'
  AND l_shipdate < DATE '1994-01-01' + INTERVAL '1' YEAR
AND l_discount BETWEEN 0.06 - 0.01 AND .06 + 0.01
AND l_quantity < 24

