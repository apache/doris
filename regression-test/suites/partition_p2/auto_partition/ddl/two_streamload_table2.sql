CREATE TABLE `two_streamload_list2`(
  `col1` bigint not null,
  `col5` bigint,
  `col2` boolean,
  `col3` tinyint,
  `col4` date
) DUPLICATE KEY(`col1`)
AUTO PARTITION BY list(`col1`)
(
)
DISTRIBUTED BY HASH(`col1`) BUCKETS 10
PROPERTIES (
"replication_num" = "1"
);