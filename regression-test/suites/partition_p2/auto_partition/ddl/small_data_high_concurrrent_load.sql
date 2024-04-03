CREATE TABLE `small_data_high_concurrent_load_range`(
  `col1` datetimev2 not null,
  `col2` varchar(128),
  `col3` decimal(9, 3),
  `col4` date
) duplicate KEY(`col1`)
auto partition by range (date_trunc(`col1`, 'day'))
(
)
DISTRIBUTED BY HASH(`col1`) BUCKETS 10
PROPERTIES (
  "replication_num" = "1"
);