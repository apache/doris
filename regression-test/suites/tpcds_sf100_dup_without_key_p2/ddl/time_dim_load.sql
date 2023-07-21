LOAD LABEL ${loadLabel} (
    DATA INFILE("s3://${s3BucketName}/regression/tpcds/sf100/time_dim.dat.gz")
    INTO TABLE time_dim
    COLUMNS TERMINATED BY "|"
    (t_time_sk, t_time_id, t_time, t_hour, t_minute, t_second, t_am_pm, t_shift, t_sub_shift, t_meal_time)
)