CREATE MATERIALIZED VIEW ssb_q31
BUILD IMMEDIATE REFRESH COMPLETE
DISTRIBUTED BY HASH(c_nation, s_nation) BUCKETS 6
PROPERTIES ('replication_num' = '1')
AS
SELECT
    c_nation,
    s_nation,
    d_year,
    SUM(lo_revenue) AS REVENUE
FROM customer, lineorder, supplier, dates
WHERE
    lo_custkey = c_custkey
    AND lo_suppkey = s_suppkey
    AND lo_orderdate = d_datekey
    AND c_region = 'ASIA'
    AND s_region = 'ASIA'
    AND d_year >= 1992
    AND d_year <= 1997
GROUP BY c_nation, s_nation, d_year
ORDER BY d_year ASC, REVENUE DESC;