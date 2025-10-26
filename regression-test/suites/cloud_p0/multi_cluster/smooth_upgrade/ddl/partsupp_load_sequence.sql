LOAD LABEL ${loadLabel} (
    DATA INFILE("s3://${s3BucketName}/regression/tpch/sf100/partsupp.tbl.*")
    INTO TABLE partsupp
    COLUMNS TERMINATED BY "|"
    (ps_partkey, ps_suppkey, ps_availqty, ps_supplycost, ps_comment, temp)
    ORDER BY ps_partkey
)