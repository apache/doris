
use demo.test_db;


drop table  if exists t_ntz_doris;
CREATE TABLE t_ntz_doris (
  col TIMESTAMP_NTZ)
USING iceberg;

drop table  if exists t_tz_doris;
CREATE TABLE t_tz_doris (
  col TIMESTAMP)
USING iceberg;
