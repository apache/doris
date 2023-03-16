LOAD LABEL ${loadLabel} (
    DATA INFILE("s3://${s3BucketName}/regression/ssb/sf100/part.tbl.gz")
    INTO TABLE part
    COLUMNS TERMINATED BY "|"
    (p_partkey,p_name,p_mfgr,p_category,p_brand,p_color,p_type,p_size,p_container,temp)
)
