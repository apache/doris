CREATE MATERIALIZED VIEW ssb_flat
BUILD IMMEDIATE REFRESH COMPLETE
DISTRIBUTED BY HASH(lo_orderkey) BUCKETS 6
PROPERTIES ('replication_num' = '1')
AS
SELECT
    LO_ORDERDATE,
    LO_ORDERKEY,
    LO_LINENUMBER,
    LO_CUSTKEY,
    LO_PARTKEY,
    LO_SUPPKEY,
    LO_ORDERPRIORITY,
    LO_SHIPPRIORITY,
    LO_QUANTITY,
    LO_EXTENDEDPRICE,
    LO_ORDTOTALPRICE,
    LO_DISCOUNT,
    LO_REVENUE,
    LO_SUPPLYCOST,
    LO_TAX,
    LO_COMMITDATE,
    LO_SHIPMODE,
    C_NAME,
    C_ADDRESS,
    C_CITY,
    C_NATION,
    C_REGION,
    C_PHONE,
    C_MKTSEGMENT,
    S_NAME,
    S_ADDRESS,
    S_CITY,
    S_NATION,
    S_REGION,
    S_PHONE,
    P_NAME,
    P_MFGR,
    P_CATEGORY,
    P_BRAND,
    P_COLOR,
    P_TYPE,
    P_SIZE,
    P_CONTAINER
FROM
lineorder as l
INNER JOIN customer c
ON (c.c_custkey = l.lo_custkey)
INNER JOIN supplier s
ON (s.s_suppkey = l.lo_suppkey)
INNER JOIN part p
ON (p.p_partkey = l.lo_partkey);