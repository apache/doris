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

-- SELECT * FROM `lakesoul`.`demo`.test_table;