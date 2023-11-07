LOAD LABEL ${loadLabel} (
    DATA INFILE("s3://${s3BucketName}/regression/ssb/sf100/customer.tbl.gz")
    INTO TABLE customer
    COLUMNS TERMINATED BY "|"
    (c_custkey,c_name,c_address,c_city,c_nation,c_region,c_phone,c_mktsegment,temp)
)
