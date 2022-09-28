LOAD LABEL ${loadLabel} (
    DATA INFILE("s3://${s3BucketName}/regression/tpch/sf10/nation.tbl")
    INTO TABLE nation
    COLUMNS TERMINATED BY "|"
    (n_nationkey, n_name, n_regionkey, n_comment, temp)
    ORDER BY n_nationkey
)
