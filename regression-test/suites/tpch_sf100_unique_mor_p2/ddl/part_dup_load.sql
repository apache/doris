LOAD LABEL ${loadLabel} (
    DATA INFILE("s3://${s3BucketName}/regression/tpch/sf100/part.tbl")
    INTO TABLE part_dup
    COLUMNS TERMINATED BY "|"
    (p_partkey, p_name, p_mfgr, p_brand, p_type, p_size, p_container, p_retailprice, p_comment, temp)
)
