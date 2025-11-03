CREATE TABLE `concurrent`(
  `col1` datetimev2 not null,
  `col2` boolean,
  `col3` tinyint,
  `col4` date,
  `col5` float,
  `col6` double,
  `col7` string,
  `col8` varchar(128),
  `col9` decimal(9, 3),
  `col10` char(128)
) duplicate KEY(`col1`)
auto partition by range (date_trunc(`col1`, 'day'))
(
)
DISTRIBUTED BY HASH(`col1`) BUCKETS 10
PROPERTIES (
  "replication_num" = "1"
);
