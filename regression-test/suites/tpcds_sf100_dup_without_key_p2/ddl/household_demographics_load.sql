LOAD LABEL ${loadLabel} (
    DATA INFILE("s3://${s3BucketName}/regression/tpcds/sf100/household_demographics.dat.gz")
    INTO TABLE household_demographics
    COLUMNS TERMINATED BY "|"
    (hd_demo_sk, hd_income_band_sk, hd_buy_potential, hd_dep_count, hd_vehicle_count)
)