SELECT
  C_COUNT,
  COUNT(*) AS CUSTDIST
FROM (
       SELECT
         CAST(C.var["C_CUSTKEY"] AS INT),
         COUNT(CAST(O.var["O_ORDERKEY"] AS INT)) AS C_COUNT
       FROM
         customer C 
         LEFT OUTER JOIN orders O ON
                                  CAST(C.var["C_CUSTKEY"] AS INT) = CAST(O.var["O_CUSTKEY"] AS INT)
                                  AND CAST(O.var["O_COMMENT"] AS TEXT) NOT LIKE '%special%requests%'
       GROUP BY
         CAST(C.var["C_CUSTKEY"] AS INT)
     ) AS C_orders
GROUP BY
  C_COUNT
ORDER BY
  CUSTDIST DESC,
  C_COUNT DESC