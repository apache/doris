create database if not exists write_test;
use write_test;

DROP TABLE IF EXISTS test_doris_write_hive_partition_table_original; 
CREATE TABLE test_doris_write_hive_partition_table_original (
  `v1` decimal(3,0), 
  `v2` string )
PARTITIONED BY ( 
  `test_date` string, 
  `v3` string)
ROW FORMAT SERDE 
  'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe' 
STORED AS INPUTFORMAT 
  'org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat' 
OUTPUTFORMAT 
  'org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat';

INSERT INTO TABLE test_doris_write_hive_partition_table_original PARTITION (test_date='2025-05-01', v3='project1')
VALUES (1, 'test1'),
       (2, 'test2');

INSERT INTO TABLE test_doris_write_hive_partition_table_original PARTITION (test_date='2025-05-01', v3='project2')
VALUES (3, 'test3');

INSERT INTO TABLE test_doris_write_hive_partition_table_original PARTITION (test_date='2025-05-02', v3='project1')
VALUES (4, 'test4');

INSERT INTO TABLE test_doris_write_hive_partition_table_original PARTITION (test_date='2025-05-02', v3='project2')
VALUES (5, 'test5'),
       (6, 'test6');
