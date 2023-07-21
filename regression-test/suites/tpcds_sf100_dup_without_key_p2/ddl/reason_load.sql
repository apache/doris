LOAD LABEL ${loadLabel} (
    DATA INFILE("s3://${s3BucketName}/regression/tpcds/sf100/reason.dat.gz")
    INTO TABLE reason
    COLUMNS TERMINATED BY "|"
    (r_reason_sk, r_reason_id, r_reason_desc)
)
