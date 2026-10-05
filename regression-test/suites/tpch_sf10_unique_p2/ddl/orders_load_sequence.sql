LOAD LABEL ${loadLabel} (
    DATA INFILE("s3://${s3BucketName}/regression/tpch/sf10/orders.tbl.*")
    INTO TABLE orders
    COLUMNS TERMINATED BY "|"
    (o_orderkey, o_custkey, o_orderstatus, o_totalprice, o_orderdate, o_orderpriority, o_clerk, o_shippriority, o_comment, temp)
    ORDER BY o_orderkey
)
