LOAD LABEL ${loadLabel} (
    DATA INFILE("s3://${s3BucketName}/regression/tpcds/sf100/income_band.dat.gz")
    INTO TABLE income_band
    COLUMNS TERMINATED BY "|"
    (ib_income_band_sk, ib_lower_bound, ib_upper_bound)
)
