-- tables: supplier,lineitem,orders,nation
SELECT /*+SEV_VAR(batch_size=3)*/
  l1.l_orderkey okey,
  l1.l_suppkey  skey
FROM
  lineitem l1
WHERE
  l1.l_receiptdate > l1.l_commitdate
  AND l1.L_ORDERKEY < 10000
  AND NOT exists(
    SELECT *
    FROM
      lineitem l3
    WHERE
      l3.l_orderkey = l1.l_orderkey
      AND l3.l_suppkey <> l1.l_suppkey
      AND l3.l_receiptdate > l3.l_commitdate
      AND l3.L_ORDERKEY < 10000
  )
ORDER BY
  okey, skey
