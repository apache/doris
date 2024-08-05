LOAD LABEL ${loadLabel} (
    DATA INFILE("s3://${s3BucketName}/regression/compaction/large_data_line/large_data_line.*.csv")
    INTO TABLE ${table_name}
    COLUMNS TERMINATED BY "|"
)
