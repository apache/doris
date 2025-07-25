SELECT  /*+SET_VAR(enable_fallback_to_original_planner=false) */
  CAST(P.var["P_BRAND"] AS TEXT),
  CAST(P.var["P_TYPE"] AS TEXT),
  CAST(P.var["P_SIZE"] AS INT),
  COUNT(DISTINCT CAST(PS.var["PS_SUPPKEY"] AS INT)) AS SUPPLIER_CNT
FROM
  partsupp PS,
  part P
WHERE
  CAST(P.var["P_PARTKEY"] AS INT) = CAST(PS.var["PS_PARTKEY"] AS INT)
  AND CAST(P.var["P_BRAND"] AS TEXT) <> 'Brand#45'
  AND CAST(P.var["P_TYPE"] AS TEXT) NOT LIKE 'MEDIUM POLISHED%'
  AND CAST(P.var["P_SIZE"] AS INT) IN (49, 14, 23, 45, 19, 3, 36, 9)
  AND CAST(PS.var["PS_SUPPKEY"] AS INT) NOT IN (
    SELECT CAST(S.var["S_SUPPKEY"] AS INT)
    FROM
      supplier S
    WHERE
      CAST(S.var["S_COMMENT"] AS TEXT) LIKE '%Customer%Complaints%'
  )
GROUP BY
  CAST(P.var["P_BRAND"] AS TEXT),
  CAST(P.var["P_TYPE"] AS TEXT),
  CAST(P.var["P_SIZE"] AS INT)
ORDER BY
  SUPPLIER_CNT DESC,
  CAST(P.var["P_BRAND"] AS TEXT),
  CAST(P.var["P_TYPE"] AS TEXT),
  CAST(P.var["P_SIZE"] AS INT)