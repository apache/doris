SET 'table.local-time-zone' = 'Asia/Shanghai';
SET 'execution.checkpointing.checkpoints-after-tasks-finish.enabled' = 'true';
-- Set the checkpointing interval
SET 'execution.checkpointing.interval' = '2s';
SET 'execution.runtime-mode' = 'batch';


CREATE CATALOG lakesoul WITH ('type'='lakesoul');

CREATE DATABASE IF NOT EXISTS `lakesoul`.`demo`;

CREATE TABLE IF NOT EXISTS `lakesoul`.`demo`.test_table (
            `id` INT,
            name STRING,
            score INT,
            `date` STRING,
            region STRING,
        PRIMARY KEY (`id`,`name`) NOT ENFORCED
        ) PARTITIONED BY (`region`,`date`)
        WITH (
            'connector'='lakesoul',
            'hashBucketNum'='4',
            'use_cdc'='true',
            'path'='s3://lakesoul-test-bucket/data/demo/test_table');

INSERT INTO `lakesoul`.`demo`.test_table VALUES (1, 'AAA', 98, '2023-05-10', 'China');

SELECT * FROM `lakesoul`.`demo`.test_table;

CREATE TABLE if not exists `lakesoul`.`demo`.customer (
  `c_custkey` int,
  `c_name` varchar(25),
  `c_address` varchar(40),
  `c_nationkey` int,
  `c_phone` char(15),
  `c_acctbal` decimal(12,2),
  `c_mktsegment` char(10),
  `c_comment` varchar(117),
  PRIMARY KEY (c_custkey, c_nationkey) NOT ENFORCED
) PARTITIONED BY (c_nationkey) WITH (
  'connector'='lakesoul',
    'hashBucketNum'='4',
);
