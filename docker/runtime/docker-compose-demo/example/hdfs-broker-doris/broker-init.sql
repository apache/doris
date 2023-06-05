LOAD LABEL test.t1_20000000 (
    DATA INFILE("hdfs://xxx.xxx.xxx.xxx:8020/user/doris/user_basic_data")
    INTO TABLE `user_basic`
    COLUMNS TERMINATED BY "," (user_id,user_name,user_msg)
)
with broker bk1 (
    "fs.defaultFS"="hdfs://xxx.xxx.xxx.xxx:8020",
    "hadoop.username"="hdfsuser"
)
PROPERTIES (
    "timeout"="1200",
    "max_filter_ratio"="0.1"
);
