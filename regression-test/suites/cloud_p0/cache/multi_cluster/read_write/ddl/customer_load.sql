LOAD LABEL ${loadLabel} (
    DATA INFILE("s3://${s3BucketName}/regression/tpch/sf100/customer.tbl")
    INTO TABLE customer
    COLUMNS TERMINATED BY "|"
    (c_custkey, c_name, c_address, c_nationkey, c_phone, c_acctbal, c_mktsegment, c_comment, temp)
)
