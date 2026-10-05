LOAD LABEL ${loadLabel} (
    DATA INFILE("s3://${s3BucketName}/regression/tpch/sf100/region.tbl")
    INTO TABLE region
    COLUMNS TERMINATED BY "|"
    (r_regionkey, r_name, r_comment, temp)
)
