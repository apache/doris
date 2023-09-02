LOAD LABEL ${loadLabel} (
    DATA INFILE("s3://${s3BucketName}/regression/tpcds/sf100/dbgen_version.dat.gz")
    INTO TABLE dbgen_version
    COLUMNS TERMINATED BY "|"
    (dv_version, dv_create_date, dv_create_time, dv_cmdline_args)
)