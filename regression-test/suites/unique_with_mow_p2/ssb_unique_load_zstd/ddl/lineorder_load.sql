LOAD LABEL ${loadLabel} (
    DATA INFILE("s3://${s3BucketName}/regression/ssb/sf100/lineorder.tbl.*.gz")
    INTO TABLE lineorder
    COLUMNS TERMINATED BY "|"
    (lo_orderkey,lo_linenumber,lo_custkey,lo_partkey,lo_suppkey,lo_orderdate,lo_orderpriority,lo_shippriority,lo_quantity,lo_extendedprice,lo_ordtotalprice,lo_discount,lo_revenue,lo_supplycost,lo_tax,lo_commitdate,lo_shipmode,temp)
)
