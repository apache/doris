LOAD LABEL ${loadLabel} (
    DATA INFILE("s3://${s3BucketName}/regression/tpch/sf100/nation.tbl")
    INTO TABLE nation_dup
    COLUMNS TERMINATED BY "|"
    (n_nationkey, n_name, n_regionkey, n_comment, temp)
)
