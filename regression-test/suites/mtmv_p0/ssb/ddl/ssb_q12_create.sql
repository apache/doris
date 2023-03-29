CREATE MATERIALIZED VIEW ssb_q12
BUILD IMMEDIATE REFRESH COMPLETE
DISTRIBUTED BY HASH(REVENUE) BUCKETS 6
PROPERTIES ('replication_num' = '1')
AS
SELECT SUM(lo_extendedprice * lo_discount) AS REVENUE
FROM lineorder, dates
WHERE
    lo_orderdate = d_datekey
    AND d_year = 1993
    AND lo_discount BETWEEN 1 AND 3
    AND lo_quantity < 25;