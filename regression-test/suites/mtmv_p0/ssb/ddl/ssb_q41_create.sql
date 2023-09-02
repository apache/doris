CREATE MATERIALIZED VIEW ssb_q41
BUILD IMMEDIATE REFRESH COMPLETE
DISTRIBUTED BY HASH(c_nation) BUCKETS 6
PROPERTIES ('replication_num' = '1')
AS
SELECT /*+SET_VAR(parallel_fragment_exec_instance_num=4, parallel_pipeline_task_num=4, batch_size=4096) */
    d_year,
    c_nation,
    SUM(lo_revenue - lo_supplycost) AS PROFIT
FROM dates, customer, supplier, part, lineorder
WHERE
    lo_custkey = c_custkey
    AND lo_suppkey = s_suppkey
    AND lo_partkey = p_partkey
    AND lo_orderdate = d_datekey
    AND c_region = 'AMERICA'
    AND s_region = 'AMERICA'
    AND (
        p_mfgr = 'MFGR#1'
        OR p_mfgr = 'MFGR#2'
    )
GROUP BY d_year, c_nation
ORDER BY d_year, c_nation;