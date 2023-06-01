LOAD LABEL ${loadLabel} (
    DATA INFILE("s3://${s3BucketName}/regression/tpcds/sf100/ship_mode.dat.gz")
    INTO TABLE ship_mode
    COLUMNS TERMINATED BY "|"
    (sm_ship_mode_sk, sm_ship_mode_id, sm_type, sm_code, sm_carrier, sm_contract)
)
