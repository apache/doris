LOAD LABEL ${loadLabel} (
    DATA INFILE("s3://${s3BucketName}/regression/tpch/sf100/supplier.tbl")
    INTO TABLE supplier
    COLUMNS TERMINATED BY "|"
    (s_suppkey, s_name, s_address, s_nationkey, s_phone, s_acctbal, s_comment, temp)
    ORDER BY s_suppkey
)
