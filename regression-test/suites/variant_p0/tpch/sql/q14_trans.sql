SELECT  /*+SET_VAR(enable_fallback_to_original_planner=false) */
100.00 * SUM(CASE
                    WHEN CAST(P.var["P_TYPE"] AS TEXT) LIKE 'PROMO%'
                      THEN CAST(L.var["L_EXTENDEDPRICE"] AS DOUBLE) * (1 - CAST(L.var["L_DISCOUNT"] AS DOUBLE))
                    ELSE 0
                    END) / SUM(CAST(L.var["L_EXTENDEDPRICE"] AS DOUBLE) * (1 - CAST(L.var["L_DISCOUNT"] AS DOUBLE))) AS PROMO_REVENUE
FROM
  lineitem L,
  part P
WHERE
  CAST(L.var["L_PARTKEY"] AS INT) = CAST(P.var["P_PARTKEY"] AS INT)
  AND CAST(L.var["L_SHIPDATE"] AS DATE) >= DATE '1995-09-01'
  AND CAST(L.var["L_SHIPDATE"] AS DATE) < DATE '1995-09-01' + INTERVAL '1' MONTH