-- TABLES: part,SUPPLIER,lineitem,orders,customer,NATION,REGION
-- ERROR: not stable
SELECT  /*+SET_VAR(enable_fallback_to_original_planner=false) */
  O_YEAR,
  SUM(CASE
      WHEN NATION = 'BRAZIL'
        THEN VOLUME
      ELSE 0
      END) / SUM(VOLUME) AS MKT_SHARE
FROM (
       SELECT
         EXTRACT(YEAR FROM CAST(O.var["O_ORDERDATE"] AS TEXT))     AS O_YEAR,
         CAST(L.var["L_EXTENDEDPRICE"] AS DOUBLE) * (1 - CAST(L.var["L_DISCOUNT"] AS DOUBLE)) AS VOLUME,
         CAST(N2.var["N_NAME"] AS TEXT)                          AS NATION
       FROM
         part P,
         supplier S,
         lineitem L,
         orders O,
         customer C,
         nation N1,
         nation N2,
         region R
       WHERE
         CAST(P.var["P_PARTKEY"] AS INT) = CAST(L.var["L_PARTKEY"] AS INT)
         AND CAST(S.var["S_SUPPKEY"] AS INT) = CAST(L.var["L_SUPPKEY"] AS INT)
         AND CAST(L.var["L_ORDERKEY"] AS INT) = CAST(O.var["O_ORDERKEY"] AS INT)
         AND CAST(O.var["O_CUSTKEY"] AS INT) = CAST(C.var["C_CUSTKEY"] AS INT)
         AND CAST(C.var["C_NATIONKEY"] AS INT) = CAST(N1.var["N_NATIONKEY"] AS INT)
         AND CAST(N1.var["N_REGIONKEY"] AS INT) = CAST(R.var["R_REGIONKEY"] AS INT)
         AND CAST(R.var["R_NAME"] AS TEXT) = 'AMERICA'
         AND CAST(S.var["S_NATIONKEY"] AS INT) = CAST(N2.var["N_NATIONKEY"] AS INT)
         AND CAST(O.var["O_ORDERDATE"] AS TEXT) BETWEEN '1995-01-01' AND '1996-12-31'
         AND CAST(P.var["P_TYPE"] AS TEXT) = 'ECONOMY ANODIZED STEEL'
     ) AS ALL_NATIONS
GROUP BY
  O_YEAR
ORDER BY
  O_YEAR
