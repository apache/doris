CREATE TABLE `stream_load_range_test_table`(
  `col1` datetimev2 not null,
  `col2` boolean,
  `col3` tinyint,
  `col4` date,
  `col5` float,
  `col6` double,
  `col7` string,
  `col8` varchar(128),
  `col9` decimal(9, 3),
  `col10` char(128),
  `col11` bigint,
  `col12` boolean,
  `col13` tinyint,
  `col14` date,
  `col15` float,
  `col16` double,
  `col17` string,
  `col18` varchar(128),
  `col19` decimal(9, 3),
  `col20` char(128),
  `col21` bigint,
  `col22` boolean,
  `col23` tinyint,
  `col24` date,
  `col25` float,
  `col26` double,
  `col27` string,
  `col28` varchar(128),
  `col29` decimal(9, 3),
  `col30` char(128),
  `col31` bigint,
  `col32` boolean,
  `col33` tinyint,
  `col34` date,
  `col35` float,
  `col36` double,
  `col37` string,
  `col38` varchar(128),
  `col39` decimal(9, 3),
  `col40` char(128)
) UNIQUE KEY(`col1`)
auto partition by range (date_trunc(`col1`, 'day'))
(
)
DISTRIBUTED BY HASH(`col1`) BUCKETS 10
PROPERTIES (
  "replication_num" = "1"
);