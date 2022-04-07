CREATE TABLE test_bucket_shuffle_join (
  `id` int NOT NULL COMMENT "",
  `rectime` datetime NOT NULL COMMENT ""
)
UNIQUE KEY(`id`,`rectime`)
COMMENT "olap"
PARTITION BY RANGE(rectime) ()
DISTRIBUTED BY HASH(id)
PROPERTIES
(
 "replication_num" = "1",
    "dynamic_partition.enable" = "true",
    "dynamic_partition.time_unit" = "MONTH",
    "dynamic_partition.end" = "2",
    "dynamic_partition.prefix" = "p",
    "dynamic_partition.buckets" = "2",
    "dynamic_partition.create_history_partition" = "true",
    "dynamic_partition.history_partition_num" = "20"
);

