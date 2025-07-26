-- TABLES: customer,orders,lineitem,SUPPLIER,NATION,REGION
SELECT  /*+SET_VAR(enable_fallback_to_original_planner=false) */
  CAST(N.var["N_NAME"] AS TEXT),
  SUM(CAST(L.var["L_EXTENDEDPRICE"] AS DOUBLE) * (1 - CAST(L.var["L_DISCOUNT"] AS DOUBLE))) AS REVENUE
FROM
  customer as C,
  orders as O,
  lineitem as L,
  supplier as S,
  nation as N,
  region as R 
WHERE
  CAST(C.var["C_CUSTKEY"] AS INT) = CAST(O.var["O_CUSTKEY"] AS INT)
  AND CAST(L.var["L_ORDERKEY"] AS INT) = CAST(O.var["O_ORDERKEY"] AS INT)
  AND CAST(L.var["L_SUPPKEY"] AS INT) = CAST(S.var["S_SUPPKEY"] AS INT)
  AND CAST(C.var["C_NATIONKEY"] AS INT) = CAST(S.var["S_NATIONKEY"] AS INT)
  AND CAST(S.var["S_NATIONKEY"] AS INT) = CAST(N.var["N_NATIONKEY"] AS INT)
  AND CAST(N.var["N_REGIONKEY"] AS INT) = CAST(R.var["R_REGIONKEY"] AS INT)
  AND CAST(R.var["R_NAME"] AS TEXT) = 'ASIA'
  AND CAST(O.var["O_ORDERDATE"] AS DATE) >= DATE '1994-01-01'
  AND CAST(O.var["O_ORDERDATE"] AS DATE) < DATE '1994-01-01' + INTERVAL '1' YEAR
GROUP BY
CAST(N.var["N_NAME"] AS TEXT)
ORDER BY
REVENUE DESC
