-- TABLES: lineitem
SELECT
  CAST(var["L_RETURNFLAG"] AS TEXT),
  CAST(var["L_LINESTATUS"] AS TEXT),
  SUM(CAST(var["L_QUANTITY"] AS DOUBLE))                                       AS SUM_QTY,
  SUM(CAST(var["L_EXTENDEDPRICE"] AS DOUBLE))                                  AS SUM_BASE_PRICE,
  SUM(CAST(var["L_EXTENDEDPRICE"] AS DOUBLE) * (1 - CAST(var["L_DISCOUNT"] AS DOUBLE)))               AS SUM_DISC_PRICE,
  SUM(CAST(var["L_EXTENDEDPRICE"] AS DOUBLE) * (1 - CAST(var["L_DISCOUNT"] AS DOUBLE)) * (1 + CAST(var["L_TAX"] AS DOUBLE))) AS SUM_CHARGE,
  AVG(CAST(var["L_QUANTITY"] AS DOUBLE))                                       AS AVG_QTY,
  AVG(CAST(var["L_EXTENDEDPRICE"] AS DOUBLE))                                  AS AVG_PRICE,
  AVG(CAST(var["L_DISCOUNT"] AS DOUBLE))                                       AS AVG_DISC,
  COUNT(*)                                              AS COUNT_ORDER
FROM
  lineitem
WHERE
  CAST(var["L_SHIPDATE"] AS DATE) <= DATE '1998-12-01' - INTERVAL '90' DAY
GROUP BY
CAST(var["L_RETURNFLAG"] AS TEXT),
CAST(var["L_LINESTATUS"] AS TEXT)
ORDER BY
CAST(var["L_RETURNFLAG"] AS TEXT),
CAST(var["L_LINESTATUS"] AS TEXT)
