LOAD LABEL ${loadLabel} (
    DATA INFILE("s3://${s3BucketName}/regression/ssb/sf100/supplier.tbl.gz")
    INTO TABLE supplier
    COLUMNS TERMINATED BY "|"
    (s_suppkey,s_name,s_address,s_city,s_nation,s_region,s_phone,temp)
)
