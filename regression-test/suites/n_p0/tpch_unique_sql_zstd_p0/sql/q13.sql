-- tables: customer
SET enable_vectorized_engine=true;
SET enable_nereids_planner=true;
SET enable_fallback_to_original_planner=false;

SELECT
  c_count,
  count(*) AS custdist
FROM (
       SELECT
         c_custkey,
         count(o_orderkey) AS c_count
       FROM
         customer
         LEFT OUTER JOIN orders ON
                                  c_custkey = o_custkey
                                  AND o_comment NOT LIKE '%special%requests%'
       GROUP BY
         c_custkey
     ) AS c_orders
GROUP BY
  c_count
ORDER BY
  custdist DESC,
  c_count DESC
