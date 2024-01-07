-- TABLES: part,SUPPLIER,lineitem,partsupp,orders,NATION
-- ERROR: not stable
SELECT
  NATION,
  O_YEAR,
  SUM(AMOUNT) AS SUM_PROFIT
FROM (
       SELECT
         CAST(N.var["N_NAME"] AS TEXT)                                                          AS NATION,
         EXTRACT(YEAR FROM CAST(O.var["O_ORDERDATE"] AS TEXT))                                  AS O_YEAR,
         CAST(L.var["L_EXTENDEDPRICE"] AS DOUBLE) * (1 - CAST(L.var["L_DISCOUNT"] AS DOUBLE)) - CAST(PS.var["PS_SUPPLYCOST"] AS DOUBLE) * CAST(L.var["L_QUANTITY"] AS DOUBLE) AS AMOUNT
       FROM
         part P,
         supplier S,
         lineitem L,
         partsupp PS,
         orders O,
         nation N
       WHERE
         CAST(S.var["S_SUPPKEY"] AS INT) = CAST(L.var["L_SUPPKEY"] AS INT)
         AND CAST(PS.var["PS_SUPPKEY"] AS INT) = CAST(L.var["L_SUPPKEY"] AS INT)
         AND CAST(PS.var["PS_PARTKEY"] AS INT) = CAST(L.var["L_PARTKEY"] AS INT)
         AND CAST(P.var["P_PARTKEY"] AS INT) = CAST(L.var["L_PARTKEY"] AS INT)
         AND CAST(O.var["O_ORDERKEY"] AS INT) = CAST(L.var["L_ORDERKEY"] AS INT)
         AND CAST(S.var["S_NATIONKEY"] AS INT) = CAST(N.var["N_NATIONKEY"] AS INT)
         AND CAST(P.var["P_NAME"] AS TEXT) LIKE '%green%'
     ) AS PROFIT
GROUP BY
  NATION,
  O_YEAR
ORDER BY
  NATION,
  O_YEAR DESC
