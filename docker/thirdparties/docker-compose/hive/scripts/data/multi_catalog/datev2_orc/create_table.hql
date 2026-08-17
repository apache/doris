create database if not exists multi_catalog;

use multi_catalog;

drop table if exists `datev2_orc`;

create external table `datev2_orc`(
  `id` int,
  `day` date)
ROW FORMAT SERDE
  'org.apache.hadoop.hive.ql.io.orc.OrcSerde'
STORED AS INPUTFORMAT
  'org.apache.hadoop.hive.ql.io.orc.OrcInputFormat'
OUTPUTFORMAT
  'org.apache.hadoop.hive.ql.io.orc.OrcOutputFormat'
LOCATION
  '/user/doris/suites/multi_catalog/datev2_orc'
TBLPROPERTIES (
  'transient_lastDdlTime'='1688118707');
