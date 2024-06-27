create database if not exists multi_catalog;

use multi_catalog;

CREATE external TABLE `datev2_csv`(
  `id` int,
  `day` date)
ROW FORMAT SERDE
  'org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe'
STORED AS INPUTFORMAT
  'org.apache.hadoop.mapred.TextInputFormat'
OUTPUTFORMAT
  'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat'
LOCATION
  '/user/doris/suites/test_hive_to_date/datev2_csv'
TBLPROPERTIES (
  'transient_lastDdlTime'='1688118691');


CREATE external TABLE `datev2_orc`(
  `id` int,
  `day` date)
ROW FORMAT SERDE
  'org.apache.hadoop.hive.ql.io.orc.OrcSerde'
STORED AS INPUTFORMAT
  'org.apache.hadoop.hive.ql.io.orc.OrcInputFormat'
OUTPUTFORMAT
  'org.apache.hadoop.hive.ql.io.orc.OrcOutputFormat'
LOCATION
  '/user/doris/suites/test_hive_to_date/datev2_orc'
TBLPROPERTIES (
  'transient_lastDdlTime'='1688118707');


CREATE external TABLE `datev2_parquet`(
  `id` int,
  `day` date)
ROW FORMAT SERDE
  'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe'
STORED AS INPUTFORMAT
  'org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat'
OUTPUTFORMAT
  'org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat'
LOCATION
  '/user/doris/suites/test_hive_to_date/datev2_parquet'
TBLPROPERTIES (
  'transient_lastDdlTime'='1688118725');

msck repair table datev2_csv;
msck repair table datev2_orc;
msck repair table datev2_parquet;

